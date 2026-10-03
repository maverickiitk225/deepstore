package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"strings"
	"testing"
)

func TestEncodeDecodePut(t *testing.T) {
	in := Record{Index: 1, OpType: OpTypePut, Key: "k", Value: "v"}
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
	in := Record{Index: 1, OpType: OpTypeDelete, Key: "k", Value: "ignored"}
	frame, err := in.Encode()
	if err != nil {
		t.Fatal(err)
	}

	var out Record
	if err := out.Decode(frame); err != nil {
		t.Fatal(err)
	}
	want := Record{Index: 1, OpType: OpTypeDelete, Key: "k", Value: ""}
	if out != want {
		t.Fatalf("round-trip: got %+v, want %+v", out, want)
	}
}

func TestEncodeDecodeCAS(t *testing.T) {
	in := Record{Index: 4, OpType: OpTypeCAS, Key: "k", Value: "new", Expected: "old"}
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

	const expOff = 1 + 8 + 1 + 4 + 1 + 4 + len("new") + 4
	if frame[8+expOff] != 'o' {
		t.Fatalf("expected byte = %q, want 'o'", frame[8+expOff])
	}
	frame[8+expOff] = 'X'
	if out.Expected != "old" {
		t.Fatal("Decode aliased expected bytes")
	}
}

func TestEncodeDecodeCASEmptyExpected(t *testing.T) {
	in := Record{Index: 1, OpType: OpTypeCAS, Key: "k", Value: "", Expected: ""}
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

func TestEncodeDecodeSession(t *testing.T) {
	in := Record{Index: 3, ClientID: 9, Seq: 2, OpType: OpTypeCAS, Key: "k", Value: "new", Expected: "old"}
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
	if frame[8] != formatV3 {
		t.Fatalf("version = %d, want %d", frame[8], formatV3)
	}
}

func TestEncodeSessionRequiresBothFields(t *testing.T) {
	_, err := Record{Index: 1, ClientID: 4, OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err == nil {
		t.Fatal("Encode client without seq: err = nil, want error")
	}
}

func TestEncodeExpectedOnPut(t *testing.T) {
	_, err := Record{Index: 1, OpType: OpTypePut, Key: "k", Value: "v", Expected: "x"}.Encode()
	if err == nil {
		t.Fatal("Encode put with expected: err = nil, want error")
	}
}

func TestEncodeEmptyKey(t *testing.T) {
	_, err := Record{OpType: OpTypePut, Key: "", Value: "v"}.Encode()
	if err == nil {
		t.Fatal("Encode empty key: err = nil, want error")
	}
}

func TestEncodePayloadTooLong(t *testing.T) {
	_, err := Record{Index: 1, OpType: OpTypePut, Key: "k", Value: strings.Repeat("x", int(maxPayloadSize))}.Encode()
	if err == nil {
		t.Fatal("Encode oversized payload: err = nil, want error")
	}
}

func TestEncodeIndexZero(t *testing.T) {
	_, err := Record{OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err == nil {
		t.Fatal("Encode index 0: err = nil, want error")
	}
}

func TestEncodeUnknownOp(t *testing.T) {
	_, err := Record{OpType: OpType("clear"), Key: "k"}.Encode()
	if err == nil {
		t.Fatal("Encode unknown op: err = nil, want error")
	}
}

func TestDecodeCRCMismatch(t *testing.T) {
	frame, err := Record{Index: 1, OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff

	var out Record
	if err := out.Decode(frame); !errors.Is(err, errCRCMismatch) {
		t.Fatalf("Decode corrupt frame: err = %v, want crc mismatch", err)
	}
}

func TestDecodeUnknownVersion(t *testing.T) {
	frame, err := Record{Index: 1, OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	frame[8] = 9
	payloadLen := binary.LittleEndian.Uint32(frame[4:8])
	payload := frame[8 : 8+payloadLen]
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, payloadLen)
	binary.LittleEndian.PutUint32(frame[0:4], crc32.ChecksumIEEE(append(lengthBuf, payload...)))

	var out Record
	if err := out.Decode(frame); err == nil {
		t.Fatal("Decode unknown version: err = nil, want error")
	}
}

func TestDecodeDoesNotAliasInput(t *testing.T) {
	frame, err := Record{Index: 1, OpType: OpTypePut, Key: "k", Value: "v"}.Encode()
	if err != nil {
		t.Fatal(err)
	}

	var out Record
	if err := out.Decode(frame); err != nil {
		t.Fatal(err)
	}

	payload := frame[8:]
	const keyOff = 1 + 8 + 1 + 4
	if len(payload) <= keyOff {
		t.Fatal("payload shorter than key")
	}
	payload[keyOff] = 'X'
	if out.Key != "k" {
		t.Fatal("Decode aliased input buffer")
	}
}
