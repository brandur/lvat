package main

import "fmt"

const (
	Prefix         = "lvat"
	CompressSuffix = "gzip"
)

func buildKey(key string, id string) string {
	return fmt.Sprintf("%s-%s-%s", Prefix, key, id)
}

func buildKeyCompressed(key string, id string) string {
	return fmt.Sprintf("%s-%s-%s-%s", Prefix, key, id, CompressSuffix)
}
