package main

import "fmt"

const (
	Prefix = "lvat"
)

func buildKey(key string, id string) string {
	return fmt.Sprintf("%s-%s-%s", Prefix, key, id)
}
