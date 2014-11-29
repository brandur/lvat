package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	conf    *IndexConf
	subject *Receiver
)

func init() {
	conf = &IndexConf{
		key:     "request_id",
		maxSize: 2,
		ttl:     1 * time.Hour,
	}
	env := map[string]string{
		"REDIS_URL": "redis://localhost:6379",
	}
	connPool = redis.NewPool(redisConnect(env["REDIS_URL"]), 1)
}

func TestMessageBuffer(t *testing.T) {
	setup(t)

	line := "request_id=req1"
	err := subject.pushAndTrim(conf, "req1", []byte(line))
	if err != nil {
		t.Error(err)
	}

	conn := connPool.Get()
	defer conn.Close()

	key := Prefix + "-request_id-req1"

	actual := redisList(t, conn, key)[0]
	if line != actual {
		t.Errorf("Expected buffer %v, got %v\n", line, actual)
	}

	ttl, err := redis.Int(conn.Do("TTL", key))
	if err != nil {
		t.Error(err)
	}

	if ttl < (CompressBuffer-10) || ttl > CompressBuffer {
		t.Errorf("Expected ttl %v, got %v\n", CompressBuffer, ttl)
	}
}

func TestMessageBufferEviction(t *testing.T) {
	setup(t)

	for i := 0; i < conf.maxSize; i++ {
		err := subject.pushAndTrim(conf, "req1",
			[]byte(fmt.Sprintf("request_id=req1 line=%i", i)))
		if err != nil {
			t.Error(err)
		}
	}

	conn := connPool.Get()
	defer conn.Close()

	key := Prefix + "-request_id-req1"

	actual, err := redis.Int(conn.Do("LLEN", key))
	if err != nil {
		t.Error(err)
	}

	if conf.maxSize != actual {
		t.Errorf("Expected buffer of size %v, got %v\n",
			conf.maxSize, actual)
	}
}

func TestMessageCompression(t *testing.T) {
	setup(t)

	line := "request_id=req1"
	err := subject.pushAndTrim(conf, "req1", []byte(line))
	if err != nil {
		t.Error(err)
	}

	conn := connPool.Get()
	defer conn.Close()

	keyCompressed := Prefix + "-request_id-req1-" + CompressSuffix

	compressed, err := redis.Bytes(conn.Do("GET", keyCompressed))
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

	ttl, err := redis.Int(conn.Do("TTL", keyCompressed))
	if err != nil {
		t.Error(err)
	}

	if ttl < (int(conf.ttl)-10) || ttl > int(conf.ttl) {
		t.Errorf("Expected ttl %v, got %v\n", int(conf.ttl), ttl)
	}
}

func TestMessageCompressionIncrement(t *testing.T) {
	setup(t)

	conn := connPool.Get()
	defer conn.Close()

	var writeBuffer bytes.Buffer
	writer := gzip.NewWriter(&writeBuffer)
	defer writer.Close()

	line := "request_id=req1"

	writer.Write([]byte(line + " line=1"))
	writer.Write([]byte("\n"))

	keyCompressed := Prefix + "-request_id-req1-" + CompressSuffix

	writer.Close()
	_, err := redis.Bytes(conn.Do("SET", keyCompressed, &writeBuffer))
	if err != nil {
		t.Error(err)
	}

	err = subject.pushAndTrim(conf, "req1", []byte(line+" line=2"))
	if err != nil {
		t.Error(err)
	}

	compressed, err := redis.Bytes(conn.Do("GET", keyCompressed))
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

func setup(t *testing.T) {
	conn := connPool.Get()
	defer conn.Close()

	_, err := conn.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}

	subject = NewReceiver(connPool)
}
