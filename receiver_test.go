package main

import (
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
	setup()

	line := "request_id=req1"
	subject.pushAndTrim(conf, "req1", []byte(line))

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

func TestMessageCompression(t *testing.T) {
	setup()
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

func setup() {
	subject = NewReceiver(connPool)
}
