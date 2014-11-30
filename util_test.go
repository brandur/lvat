package main

import "testing"

func TestBuildKey(t *testing.T) {
	actual := buildKey("request_id", "req1")
	expected := Prefix + "-request_id-req1"
	if expected != actual {
		t.Errorf("Expected key %v, got %v\n", expected, actual)
	}
}
