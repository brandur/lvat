package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/garyburd/redigo/redis"
)

type Receiver struct {
	MessagesChan chan *LogMessage
	connPool     *redis.Pool
}

func NewReceiver(connPool *redis.Pool) *Receiver {
	return &Receiver{
		MessagesChan: make(chan *LogMessage, BufferSize),
		connPool:     connPool,
	}
}

func (r *Receiver) Run() {
	for i := 0; i < Concurrency; i++ {
		go r.handleMessage()
	}
}

func (r *Receiver) handleMessage() {
	for message := range r.MessagesChan {
		for _, conf := range confs {
			if value, ok := message.pairs[conf.key]; ok {
				err := r.pushAndTrim(&conf, value, message.data)
				if err != nil {
					fmt.Fprintf(os.Stderr,
						"Couldn't push message to Redis: %s\n", err.Error())
				}
			}
		}
	}
}

func (r *Receiver) pushAndTrim(conf *IndexConf, value string, line []byte) error {
	conn := r.connPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("%s-%s-%s", Prefix, conf.key, value)
	conn.Send("MULTI")

	// push the line in and trim the list to its maximum length
	conn.Send("LPUSH", key, line)
	conn.Send("LTRIM", key, 0, conf.maxSize-1)

	conn.Send("EXPIRE", key, time.Now().Add(CompressBuffer))

	_, err := conn.Do("EXEC")
	if err != nil {
		return err
	}

	keyCompressed := key + "-" + CompressSuffix

	conn.Send("WATCH", keyCompressed)

	compressed, err := redis.Bytes(conn.Do("GET", keyCompressed))
	if err != nil {
		return err
	}

	var writeBuffer bytes.Buffer
	writer := gzip.NewWriter(&writeBuffer)
	defer writer.Close()

	// read in whatever we already have compressed and write it out to
	// the our write buffer
	reader, err := gzip.NewReader(bytes.NewBuffer(compressed))
	defer reader.Close()
	io.Copy(writer, reader)

	writer.Write([]byte(value + "\n"))

	conn.Send("MULTI")

	// store in the compressed fragments
	conn.Send("SET", keyCompressed, writeBuffer)

	// bump the key's TTL now that it has a new entry
	conn.Send("EXPIRE", keyCompressed, time.Now().Add(conf.ttl))

	_, err = conn.Do("EXEC")
	if err != nil {
		return err
	}

	return nil
}
