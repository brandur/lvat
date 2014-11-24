package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/bmizerany/lpx"
	"github.com/garyburd/redigo/redis"
	"github.com/kr/logfmt"
)

const (
	BufferSize  = 1000
	Concurrency = 50
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

// come Go 1.4 switch this out for r.BasicAuth ...
func basicAuthPassword(r *http.Request) string {
	auth := r.Header.Get("Authorization")

	i := strings.IndexRune(auth, ' ')
	if i < 0 || auth[0:i] != "Basic" {
		return ""
	}

	buffer, err := base64.StdEncoding.DecodeString(auth[i+1:])
	if err != nil {
		return ""
	}

	credentials := string(buffer)
	i = strings.IndexRune(credentials, ':')
	if i < 0 {
		return ""
	}

	return credentials[i+1:]
}

func handleMessage() {
	for message := range messagesChan {
		for _, conf := range confs {
			if value, ok := message.pairs[conf.key]; ok {
				err := pushAndTrim(&conf, value, message.data)
				if err != nil {
					fmt.Fprintf(os.Stderr,
					  "Couldn't push message to Redis: %s\n", err.Error())
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

	apiKey := os.Getenv("API_KEY")
	port := os.Getenv("PORT")
	redisUrl := os.Getenv("REDIS_URL")

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

	connPool = redis.NewPool(func() (redis.Conn, error) {
		u, err := url.Parse(redisUrl)
		if err != nil {
			return nil, err
		}

		password := ""
		passwordProvided := false
		if u.User != nil {
			password, passwordProvided = u.User.Password()
		}

		conn, err := redis.Dial("tcp", u.Host)
		if err != nil {
			return nil, err
		}

		if passwordProvided {
			if _, err := conn.Do("AUTH", password); err != nil {
				conn.Close()
				return nil, err
			}
		}

		return conn, err
	}, Concurrency)
	defer connPool.Close()

	for i := 0; i < Concurrency; i++ {
		go handleMessage()
	}

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
