package convert

import (
	"bytes"
	"testing"

	"github.com/deepanker/deepstore/internal/store"
)

func TestCommandRoundTrip(t *testing.T) {
	cmd := store.Command{
		Type:     store.CommandTypeCAS,
		Key:      "k",
		Value:    "v",
		Expected: "",
		ClientID: 7,
		Seq:      3,
		Index:    99,
	}
	buf, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeCommand(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != 0 || got.Type != cmd.Type || got.Key != cmd.Key || got.Value != cmd.Value || got.Expected != cmd.Expected || got.ClientID != cmd.ClientID || got.Seq != cmd.Seq {
		t.Fatalf("decode = %+v", got)
	}
	again, err := EncodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, again) {
		t.Fatal("encode is not stable")
	}
}

func TestAdvertiseRoundTrip(t *testing.T) {
	buf, err := EncodeAdvertise("node2", "http://127.0.0.1:7002")
	if err != nil {
		t.Fatal(err)
	}
	id, addr, err := DecodeAdvertise(buf)
	if err != nil {
		t.Fatal(err)
	}
	if id != "node2" || addr != "http://127.0.0.1:7002" {
		t.Fatalf("decode = (%q, %q)", id, addr)
	}
	if _, err := EncodeAdvertise("", "http://127.0.0.1:7002"); err == nil {
		t.Fatal("empty id: err = nil")
	}
}

func TestCommandRejectsEmptyKey(t *testing.T) {
	_, err := EncodeCommand(store.Command{Type: store.CommandTypePut, Value: "v", ClientID: 1, Seq: 1})
	if err == nil {
		t.Fatal("empty key: err = nil")
	}
}
