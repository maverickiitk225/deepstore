package wal

import (
	"bytes"
	"testing"
)

func TestEncodeDecodePut(t *testing.T) {
	in := Record{OpType: OpTypePut, Key: "k", Value: "v"}
	frame, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}

	var out Record
	if err := out.Decode(frame); err != nil {
		t.Fatal(err)
	}
	if out != in {
		t.Fatalf("round-trip: got %+v, want %+v", out, in)
	}
}

func TestEncodeDecodeDelete(t *testing.T) {
	in := Record{OpType: OpTypeDelete, Key: "k", Value: "ignored"}
	frame, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}

	var out Record
	if err := out.Decode(frame); err != nil {
		t.Fatal(err)
	}
	want := Record{OpType: OpTypeDelete, Key: "k", Value: ""}
	if out != want {
		t.Fatalf("round-trip: got %+v, want %+v", out, want)
	}
}

func TestEncodeEmptyKey(t *testing.T) {
	_, err := Record{OpType: OpTypePut, Key: "", Value: "v"}.Encode()
	if err == nil {
		t.Fatal("Encode empty key: err = nil, want error")
	}
}

func TestEncodeUnknownOp(t *testing.T) {
	_, err := Record{OpType: OpType("clear"), Key: "k"}.Encode()
	if err == nil {
		t.Fatal("Encode unknown op: err = nil, want error")
	}
}

func TestDecodeCRCMismatch(t *testing.T) {
	frame, err := Record{OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff

	var out Record
	if err := out.Decode(frame); err == nil {
		t.Fatal("Decode corrupt frame: err = nil, want error")
	}
}

func TestDecodeDoesNotAliasInput(t *testing.T) {
	frame, err := Record{OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err != nil {
		t.Fatal(err)
	}

	var out Record
	if err := out.Decode(frame); err != nil {
		t.Fatal(err)
	}

	payload := frame[8:]
	if len(payload) > 6 {
		payload[6] = 'X'
	}
	if out.Key == "X" || bytes.Contains([]byte(out.Key), []byte("X")) {
		t.Fatal("Decode aliased input buffer")
	}
}
