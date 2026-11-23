package node

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
	buf, err := encodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCommand(buf)
	if err != nil {
		t.Fatal(err)
	}
	if got.Index != 0 || got.Type != cmd.Type || got.Key != cmd.Key || got.Value != cmd.Value || got.Expected != cmd.Expected || got.ClientID != cmd.ClientID || got.Seq != cmd.Seq {
		t.Fatalf("decode = %+v", got)
	}
	again, err := encodeCommand(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf, again) {
		t.Fatal("encode is not stable")
	}
}

func TestCommandRejectsEmptyKey(t *testing.T) {
	_, err := encodeCommand(store.Command{Type: store.CommandTypePut, Value: "v", ClientID: 1, Seq: 1})
	if err == nil {
		t.Fatal("empty key: err = nil")
	}
}
