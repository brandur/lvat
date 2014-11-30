package main

import "github.com/garyburd/redigo/redis"

type Retriever struct {
	confs    []*IndexConf
	connPool *redis.Pool
}

func NewRetriever(confs []*IndexConf, connPool *redis.Pool) *Retriever {
	return &Retriever{
		confs:    confs,
		connPool: connPool,
	}
}

func (r *Retriever) Lookup(query string) ([]byte, bool, error) {
	conn := connPool.Get()
	defer conn.Close()

	// Move through each type of message stored until there is a match. If
	// there is never a match, return a 404.
	for _, conf := range r.confs {
		key := buildKey(conf.key, query)
		compressed, err := conn.Do("GET", key)
		if err != nil {
			return nil, false, err
		}

		if compressed == nil {
			continue
		}

		return compressed.([]byte), true, nil
	}

	return nil, false, nil
}
