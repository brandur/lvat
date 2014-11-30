package main

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/garyburd/redigo/redis"
)

const (
	Prefix = "lvat"
)

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

func buildKey(key string, id string) string {
	return fmt.Sprintf("%s-%s-%s", Prefix, key, id)
}

func redisConnect(redisUrl string) func() (redis.Conn, error) {
	return func() (redis.Conn, error) {
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
	}
}
