package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"math/rand"
	"io"
	"os"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
)

const (
	BufferSize     = 200
	CompressBuffer = 300
	LockRetries    = 5
)

type Receiver struct {
	MessagesChan chan []*LogMessage
	confs        []*IndexConf
	connPool     *redis.Pool
}

type StorageGroup map[*IndexConf]map[string][][]byte

func NewReceiver(confs []*IndexConf, connPool *redis.Pool) *Receiver {
	return &Receiver{
		MessagesChan: make(chan []*LogMessage, BufferSize),
		confs:        confs,
		connPool:     connPool,
	}
}

func (r *Receiver) Run() {
	for i := 0; i < Concurrency; i++ {
		go r.handleMessage()
	}
}

// Batch lines up as groups before inserting anything so that we can
// minimize the number of Redis transactions that we have to perform.
//
// Batches of messages from Logplex are not guaranteed by any means to
// all originate from a single request or even be related at all, but in
// practice they are more often than not.
func (r *Receiver) buildGroups(messages []*LogMessage) StorageGroup {
	groups := make(StorageGroup)

	for _, message := range messages {
		for _, conf := range r.confs {
			if value, ok := message.pairs[conf.key]; ok {
				if _, ok = groups[conf]; !ok {
					groups[conf] = make(map[string][][]byte)
				}

				// assume that multiple values split by commas are
				// different identifiers that should be stored separately
				for _, subValue := range strings.Split(value, ",") {
					if _, ok = groups[conf][subValue]; !ok {
						groups[conf][subValue] = make([][]byte, 0, 1)
					}

					groups[conf][subValue] =
						append(groups[conf][subValue], message.data)
				}
			}
		}
	}

	return groups
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
			// sleep for a random small amount of time to help avoid
			// contention problems with other parallel processes that are
			// also trying to push data to this key
			sleepDuration := time.Duration(rand.Intn(10)) * time.Millisecond

			fmt.Printf("transaction_failed attempt=%v sleep=%v\n",
				i, sleepDuration)
			time.Sleep(sleepDuration)
			continue
		}
		break
	}
	return err
}

func (r *Receiver) compressOptimistically(conf *IndexConf, value string, lines [][]byte) (bool, error) {
	conn := r.connPool.Get()
	defer conn.Close()

	key := buildKey(conf.key, value)

	conn.Send("WATCH", key)

	compressed, err := conn.Do("GET", key)
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
	conn.Send("SET", key, &writeBuffer)

	// bump the key's TTL now that it has a new entry
	conn.Send("EXPIRE", key, int(conf.ttl))

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
		for conf, confGroups := range r.buildGroups(messages) {
			for value, lines := range confGroups {
				printVerbose("handle_group key=%v value=%v size=%v\n",
					conf.key, value, len(lines))

				err := r.compress(conf, value, lines)
				if err != nil {
					fmt.Fprintf(os.Stderr,
						"Couldn't compress message to Redis: %s\n", err.Error())
				}
			}
		}
	}
}
