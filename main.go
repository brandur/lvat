package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/bmizerany/lpx"
	"github.com/garyburd/redigo/redis"
	"github.com/kr/logfmt"
)

const (
	Concurrency = 40
)

var (
	confs     []*IndexConf
	connPool  *redis.Pool
	receiver  *Receiver
	retriever *Retriever
	verbose   bool
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

func receiveMessage(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	messages := make([]*LogMessage, 0)
	lp := lpx.NewReader(bufio.NewReader(r.Body))
	for lp.Next() {
		message := &LogMessage{
			data:  bytes.TrimSpace(lp.Bytes()),
			pairs: make(map[string]string),
		}
		err := logfmt.Unmarshal(message.data, message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't unmarshal message: "+err.Error())
			continue
		}
		messages = append(messages, message)
	}

	printVerbose("queue_messages num=%v\n", len(messages))

	// send through the whole set of messages at once to reduce the
	// probability of inter-routine contention
	receiver.MessagesChan <- messages
	printVerbose("queue size=%v\n", len(receiver.MessagesChan))
}

func lookupMessages(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	query := r.FormValue("query")
	if query == "" {
		w.WriteHeader(400)
		w.Write([]byte("Need `query` parameter."))
		return
	}

	content, ok, err := retriever.Lookup(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Couldn't perform lookup: "+err.Error())
		w.WriteHeader(500)
		return
	}

	if !ok {
		w.WriteHeader(404)
		return
	}

	// write directly if the client supports gzip, and a string
	// directly otherwise
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		w.Write(content)
	} else {
		reader, err := gzip.NewReader(bytes.NewBuffer(content))
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't unpack: "+err.Error())
			w.WriteHeader(500)
			return
		}
		defer reader.Close()
		io.Copy(w, reader)
	}
}

func init() {
	confs = []*IndexConf{
		&IndexConf{
			key:     "request_id",
			maxSize: 500,
			ttl:     48 * time.Hour,
		},
	}
}

func main() {
	var err error

	apiKey := os.Getenv("API_KEY")
	port := os.Getenv("PORT")
	redisUrl := os.Getenv("REDIS_URL")

	// support special alternate configs for now
	if redisUrl == "" {
		redisUrl = os.Getenv("OPENREDIS_URL")
	}

	if apiKey == "" {
		err = fmt.Errorf("Need API_KEY")
		goto exit
	}
	if port == "" {
		err = fmt.Errorf("Need PORT")
		goto exit
	}
	if redisUrl == "" {
		err = fmt.Errorf("Need REDIS_URL")
		goto exit
	}

	if os.Getenv("VERBOSE") == "true" {
		verbose = true
	}

	connPool = redis.NewPool(redisConnect(redisUrl), Concurrency)
	defer connPool.Close()

	receiver = NewReceiver(confs, connPool)
	receiver.Run()

	retriever = NewRetriever(confs, connPool)

	http.HandleFunc("/messages", func(w http.ResponseWriter, r *http.Request) {
		if basicAuthPassword(r) != apiKey {
			w.WriteHeader(401)
			return
		}

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
		goto exit
	}

exit:
	if err != nil {
		fmt.Fprintf(os.Stderr, err.Error()+"\n")
		defer os.Exit(1)
	}
}

func printVerbose(message string, args ...interface{}) {
	if verbose {
		fmt.Printf(message, args...)
	}
}
