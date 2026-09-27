package wal

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/deepanker/deepstore/internal/store"
)

func TestWALAppendReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{OpType: OpTypePut, Key: "b", Value: "2"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	w, err = Open(dir, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(got) != 2 {
		t.Fatalf("replay count = %d, want 2", len(got))
	}
	if got[0].Key != "a" || got[0].Value != "1" {
		t.Fatalf("first record: %+v", got[0])
	}
	if got[1].Key != "b" || got[1].Value != "2" {
		t.Fatalf("second record: %+v", got[1])
	}
}

func TestWALReplayAppliesToStateMachine(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append(Record{OpType: OpTypePut, Key: "k", Value: "v"})
	_ = w.Sync()
	_ = w.Close()

	sm := store.NewStateMachine()
	_, err = Open(dir, func(r Record) error {
		cmd := store.Command{Key: r.Key, Value: r.Value}
		switch r.OpType {
		case OpTypePut:
			cmd.Type = store.CommandTypePut
		case OpTypeDelete:
			cmd.Type = store.CommandTypeDelete
		default:
			t.Fatalf("unexpected op: %s", r.OpType)
		}
		return sm.Apply(cmd)
	})
	if err != nil {
		t.Fatal(err)
	}

	val, ok := sm.Get("k")
	if !ok || val != "v" {
		t.Fatalf("Get(k) = (%q, %v), want (v, true)", val, ok)
	}
}

func TestWALTornTailTruncates(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{OpType: OpTypePut, Key: "ok", Value: "yes"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{OpType: OpTypePut, Key: "drop", Value: "me"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, walFileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	var got []Record
	w, err = Open(dir, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()

	if len(got) != 1 || got[0].Key != "ok" {
		t.Fatalf("after torn tail: got %+v, want one put ok", got)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("expected non-empty wal after truncating torn record")
	}
}

func TestWALCorruptMiddleFails(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append(Record{OpType: OpTypePut, Key: "a", Value: "1"})
	_ = w.Append(Record{OpType: OpTypePut, Key: "b", Value: "2"})
	_ = w.Sync()
	_ = w.Close()

	path := filepath.Join(dir, walFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir, func(r Record) error { return nil })
	if err == nil {
		t.Fatal("Open corrupt wal: err = nil, want error")
	}
}

func TestWALPayloadLengthOverMaxIsCorruption(t *testing.T) {
	for _, length := range []uint32{maxPayloadSize + 1, ^uint32(0)} {
		t.Run(fmt.Sprintf("%d", length), func(t *testing.T) {
			dir := t.TempDir()

			w, err := Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Append(Record{OpType: OpTypePut, Key: "ok", Value: "1"}); err != nil {
				t.Fatal(err)
			}
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			path := filepath.Join(dir, walFileName)
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
			if err != nil {
				t.Fatal(err)
			}
			var hdr [8]byte
			binary.LittleEndian.PutUint32(hdr[4:8], length)
			if _, err := f.Write(hdr[:]); err != nil {
				t.Fatal(err)
			}
			if err := f.Close(); err != nil {
				t.Fatal(err)
			}

			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			before := info.Size()

			_, err = Open(dir, func(Record) error { return nil })
			if err == nil {
				t.Fatal("Open oversized length: err = nil, want error")
			}

			info, err = os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != before {
				t.Fatalf("wal size = %d, want %d", info.Size(), before)
			}
		})
	}
}
