package main

import (
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
)

var (
	conf *IndexConf
)

func init() {
	env := map[string]string{
		"REDIS_URL": "redis://localhost:6379",
	}
	connPool = redis.NewPool(redisConnect(env["REDIS_URL"]), 1)

	conf = &IndexConf{
		key:     "request_id",
		maxSize: 2,
		ttl:     1 * time.Hour,
	}
}

func setup(t *testing.T) {
	conn := connPool.Get()
	defer conn.Close()

	_, err := conn.Do("FLUSHALL")
	if err != nil {
		t.Error(err)
	}
}
