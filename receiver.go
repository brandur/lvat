package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"

	"github.com/garyburd/redigo/redis"
)

const (
	BufferSize     = 1000
	CompressBuffer = 300
	LockRetries    = 5
)

type Receiver struct {
	MessagesChan chan []*LogMessage
	connPool     *redis.Pool
}

func NewReceiver(connPool *redis.Pool) *Receiver {
	return &Receiver{
		MessagesChan: make(chan []*LogMessage, BufferSize),
		connPool:     connPool,
	}
}

func (r *Receiver) Run() {
	for i := 0; i < Concurrency; i++ {
		go r.handleMessage()
	}
}

func (r *Receiver) compress(conf *IndexConf, value string, lines [][]byte) error {
	var err error
	// We use an optimistic locking strategy to set our compressed traces
	// by assuming that another routing/process isn't trying to set the
	// same value. On a locking failure, try again a number of times
	// before giving up.
	for i := 0; i < LockRetries; i++ {
		ok, err := r.compressOptimistically(conf, value, lines)
		if err == nil && !ok {
			fmt.Printf("WATCH failed; retrying set\n")
			continue
		}
		break
	}
	return err
}

func (r *Receiver) compressOptimistically(conf *IndexConf, value string, lines [][]byte) (bool, error) {
	conn := r.connPool.Get()
	defer conn.Close()

	keyCompressed := buildKeyCompressed(conf.key, value)

	conn.Send("WATCH", keyCompressed)

	compressed, err := conn.Do("GET", keyCompressed)
	if err != nil {
		return true, err
	}

	var writeBuffer bytes.Buffer
	writer := gzip.NewWriter(&writeBuffer)
	defer writer.Close()

	// read in whatever we already have compressed and write it out to
	// the our write buffer
	if compressed != nil {
		reader, err := gzip.NewReader(bytes.NewBuffer(compressed.([]byte)))
		if err != nil {
			return true, err
		}
		defer reader.Close()
		io.Copy(writer, reader)
	}

	for _, line := range lines {
		writer.Write(line)
		writer.Write([]byte("\n"))
	}

	conn.Send("MULTI")

	// store in the compressed fragments
	writer.Close()
	conn.Send("SET", keyCompressed, &writeBuffer)

	// bump the key's TTL now that it has a new entry
	conn.Send("EXPIRE", keyCompressed, int(conf.ttl))

	res, err := conn.Do("EXEC")
	// if the WATCH failed, then EXEC will return nil instead of
	// individual execution results
	if res == nil {
		return false, nil
	}
	return true, err
}

func (r *Receiver) handleMessage() {
	for messages := range r.MessagesChan {
		groups := make(map[*IndexConf]map[string][][]byte)

		// batch lines up as groups before inserting anything so that we
		// can minimize the number of Redis transactions that we have to
		// perform
		for _, message := range messages {
			for _, conf := range confs {
				if value, ok := message.pairs[conf.key]; ok {
					if _, ok = groups[conf]; !ok {
						groups[conf] = make(map[string][][]byte)
					}

					if _, ok = groups[conf][value]; !ok {
						groups[conf][value] = make([][]byte, 0)
					}

					groups[conf][value] =
						append(groups[conf][value], message.data)
				}
			}
		}

		for conf, confGroups := range groups {
			for value, lines := range confGroups {
				printVerbose("handle_group key=%v value=%v size=%v\n",
					conf.key, value, len(lines))

				err := r.pushAndTrim(conf, value, lines)
				if err != nil {
					fmt.Fprintf(os.Stderr,
						"Couldn't push message to Redis: %s\n", err.Error())
				}

				err = r.compress(conf, value, lines)
				if err != nil {
					fmt.Fprintf(os.Stderr,
						"Couldn't compress message to Redis: %s\n", err.Error())
				}
			}
		}
	}
}

func (r *Receiver) pushAndTrim(conf *IndexConf, value string, lines [][]byte) error {
	conn := r.connPool.Get()
	defer conn.Close()

	key := buildKey(conf.key, value)
	conn.Send("MULTI")

	// push the line in and trim the list to its maximum length
	for _, line := range lines {
		conn.Send("LPUSH", key, line)
	}

	conn.Send("LTRIM", key, 0, conf.maxSize-1)

	conn.Send("EXPIRE", key, CompressBuffer)

	_, err := conn.Do("EXEC")
	return err
}
