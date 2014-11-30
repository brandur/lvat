package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"testing"

	"github.com/garyburd/redigo/redis"
)

func TestBuildGroups(t *testing.T) {
	setup(t)

	subject := NewReceiver([]*IndexConf{conf}, connPool)

	messages := []*LogMessage{
		&LogMessage{
			data: []byte("request_id=req1 line=1"),
			pairs: map[string]string{
				"request_id": "req1",
				"line": "1",
			},
		},
		&LogMessage{
			data: []byte("request_id=req1,req2 line=2"),
			pairs: map[string]string{
				"request_id": "req1,req2",
				"line": "2",
			},
		},
		&LogMessage{
			data: []byte("request_id=req2 line=1"),
			pairs: map[string]string{
				"request_id": "req2",
				"line": "1",
			},
		},
	}

	groups := subject.buildGroups(messages)
	if len(groups) != 1 {
		t.Errorf("Expected group length %v, got %v\n", 1, len(groups))
	}

	confGroup := groups[conf]
	if len(confGroup) != 2 {
		t.Errorf("Expected conf group length %v, got %v\n", 2, len(confGroup))
	}
		
	lines := confGroup["req1"]
	if len(lines) != 2 {
		t.Errorf("Expected req1 lines length %v, got %v\n", 2, len(lines))
	}
		
	lines = confGroup["req2"]
	if len(lines) != 2 {
		t.Errorf("Expected req2 lines length %v, got %v\n", 2, len(lines))
	}
}

func TestMessageCompression(t *testing.T) {
	setup(t)

	subject := NewReceiver([]*IndexConf{conf}, connPool)

	line := "request_id=req1"
	err := subject.compress(conf, "req1", [][]byte{[]byte(line)})
	if err != nil {
		t.Error(err)
	}

	conn := connPool.Get()
	defer conn.Close()

	key := buildKey("request_id", "req1")

	compressed, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		t.Error(err)
	}

	reader, err := gzip.NewReader(bytes.NewBuffer(compressed))
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Error(err)
	}

	actual := string(b)
	expected := line + "\n"
	if expected != actual {
		t.Errorf("Expected buffer '%v', got '%v'\n", expected, actual)
	}

	ttl, err := redis.Int(conn.Do("TTL", key))
	if err != nil {
		t.Error(err)
	}

	if ttl < (int(conf.ttl)-10) || ttl > int(conf.ttl) {
		t.Errorf("Expected ttl %v, got %v\n", int(conf.ttl), ttl)
	}
}

func TestMessageCompressionIncrement(t *testing.T) {
	setup(t)

	subject := NewReceiver([]*IndexConf{conf}, connPool)

	conn := connPool.Get()
	defer conn.Close()

	var writeBuffer bytes.Buffer
	writer := gzip.NewWriter(&writeBuffer)
	defer writer.Close()

	line := "request_id=req1"

	writer.Write([]byte(line + " line=1"))
	writer.Write([]byte("\n"))

	key := buildKey("request_id", "req1")

	writer.Close()
	_, err := redis.Bytes(conn.Do("SET", key, &writeBuffer))
	if err != nil {
		t.Error(err)
	}

	err = subject.compress(conf, "req1", [][]byte{[]byte(line + " line=2")})
	if err != nil {
		t.Error(err)
	}

	compressed, err := redis.Bytes(conn.Do("GET", key))
	if err != nil {
		t.Error(err)
	}

	reader, err := gzip.NewReader(bytes.NewBuffer(compressed))
	if err != nil {
		t.Error(err)
	}
	defer reader.Close()

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Error(err)
	}

	actual := string(b)
	expected := line + " line=1\n" + line + " line=2\n"
	if expected != actual {
		t.Errorf("Expected buffer '%v', got '%v'\n", expected, actual)
	}
}

func redisList(t *testing.T, conn redis.Conn, key string) []string {
	results, err := redis.Values(conn.Do("LRANGE", key, 0, 1))
	if err != nil {
		t.Error(err)
	}

	strings := make([]string, len(results))
	for i := 0; i < len(results); i++ {
		j := len(results) - 1 - i
		strings[i] = string(results[j].([]byte))
	}

	return strings
}
