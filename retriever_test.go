package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"testing"
)

func TestLookup(t *testing.T) {
	setup(t)

	receiver := NewReceiver(connPool)
	retriever := NewRetriever([]*IndexConf{conf}, connPool)

	_, ok, err := retriever.Lookup("req1")
	if err != nil {
		t.Error(err)
	}
	if ok {
		t.Errorf("Expected found false, got true\n")
	}

	line := "request_id=req1"
	err = receiver.compress(conf, "req1", [][]byte{[]byte(line)})
	if err != nil {
		t.Error(err)
	}

	content, ok, err := retriever.Lookup("req1")
	if err != nil {
		t.Error(err)
	}
	if !ok {
		t.Errorf("Expected found true, got false\n")
	}

	reader, err := gzip.NewReader(bytes.NewBuffer(content))
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
}
