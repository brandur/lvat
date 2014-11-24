package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/bmizerany/lpx"
	"github.com/garyburd/redigo/redis"
	"github.com/kr/logfmt"
)

const (
	BufferSize  = 1000
	Concurrency = 10
	Prefix      = "ltank"
)

var (
	confs        []IndexConf
	connPool     *redis.Pool
	messagesChan chan *LogMessage
)

type IndexConf struct {
	key     string
	maxSize int
	ttl     time.Duration
}

type LogMessage struct {
	data  []byte
	pairs map[string]string
}

func (m *LogMessage) HandleLogfmt(key, value []byte) error {
	m.pairs[string(key)] = string(value)
	return nil
}

func handleMessage() {
	for message := range messagesChan {
		fmt.Printf("%v\n", message.pairs)

		for _, conf := range confs {
			if value, ok := message.pairs[conf.key]; ok {
				err := pushAndTrim(&conf, value, message.data)
				if err != nil {
					// TODO: this will shut down the Goroutine and it will
					// never come back up
					panic(err)
				}
			}
		}
	}
}

func receiveMessage(w http.ResponseWriter, r *http.Request) {
	lp := lpx.NewReader(bufio.NewReader(r.Body))
	defer r.Body.Close()
	for lp.Next() {
		message := &LogMessage{
			data:  bytes.TrimSpace(lp.Bytes()),
			pairs: make(map[string]string),
		}
		err := logfmt.Unmarshal(message.data, message)
		if err != nil {
			panic(err)
		}
		messagesChan <- message
	}
}

func lookupMessages(w http.ResponseWriter, r *http.Request) {
	query := r.FormValue("query")
	if query == "" {
		w.WriteHeader(400)
		w.Write([]byte("Need `query` parameter."))
		return
	}

	conn := connPool.Get()
	defer conn.Close()

	for _, conf := range confs {
		key := fmt.Sprintf("%s-%s-%s", Prefix, conf.key, query)

		results, err := redis.Values(conn.Do("LRANGE", key, 0, conf.maxSize-1))
		if err != nil {
			panic(err)
		}

		// store elements in reverse because we've stored trace lines in Redis
		// backwards
		strings := make([]string, len(results))
		for i := 0; i < len(results); i++ {
			j := len(results) - 1 - i
			strings[i] = string(results[j].([]byte))
		}

		resp, err := json.Marshal(strings)
		if err != nil {
			panic(err)
		}

		w.Write(resp)
		return
	}

	w.WriteHeader(404)
}

func init() {
	messagesChan = make(chan *LogMessage, BufferSize)

	confs = []IndexConf{
		IndexConf{
			key:     "request_id",
			maxSize: 500,
			ttl:     48 * time.Hour,
		},
	}
}

func main() {
	var err error

	port := os.Getenv("PORT")
	if port == "" {
		err = fmt.Errorf("Need PORT")
		goto exit
	}

	connPool = redis.NewPool(func() (redis.Conn, error) {
		return redis.Dial("tcp", ":6379")
	}, Concurrency)
	defer connPool.Close()

	for i := 0; i < Concurrency; i++ {
		go handleMessage()
	}

	http.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case "GET":
			lookupMessages(w, r)
		case "POST":
			receiveMessage(w, r)
		default:
			w.WriteHeader(404)
			return
		}
	})
	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		panic(err)
	}

exit:
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		defer os.Exit(1)
	}
}

func pushAndTrim(conf *IndexConf, value string, line []byte) error {
	conn := connPool.Get()
	defer conn.Close()

	key := fmt.Sprintf("%s-%s-%s", Prefix, conf.key, value)
	fmt.Printf("Setting %s\n", key)
	conn.Send("MULTI")

	// push the line in and trim the list to its maximum length
	conn.Send("LPUSH", key, line)
	conn.Send("LTRIM", key, 0, conf.maxSize-1)

	// bump the key's TTL now that it has a new entry
	conn.Send("EXPIRE", key, time.Now().Add(conf.ttl))

	_, err := conn.Do("EXEC")
	return err
}
