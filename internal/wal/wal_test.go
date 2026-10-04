package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
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
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 2, OpType: OpTypePut, Key: "b", Value: "2"}); err != nil {
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
	if got[0].Index != 1 || got[0].Key != "a" || got[0].Value != "1" {
		t.Fatalf("first record: %+v", got[0])
	}
	if got[1].Index != 2 || got[1].Key != "b" || got[1].Value != "2" {
		t.Fatalf("second record: %+v", got[1])
	}
}

func TestWALAppendManyOneWrite(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"},
		{Index: 2, OpType: OpTypeDelete, Key: "a"},
		{Index: 3, OpType: OpTypePut, Key: "b", Value: "3"},
	}
	if err := w.AppendMany(recs); err != nil {
		t.Fatal(err)
	}
	if w.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", w.LastIndex())
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
	if len(got) != len(recs) {
		t.Fatalf("replay count = %d, want %d", len(got), len(recs))
	}
	for i := range recs {
		if got[i].Index != recs[i].Index || got[i].OpType != recs[i].OpType || got[i].Key != recs[i].Key || got[i].Value != recs[i].Value {
			t.Fatalf("record %d: got %+v, want %+v", i, got[i], recs[i])
		}
	}
}

func TestWALReplayAppliesToStateMachine(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = w.Append(Record{Index: 1, OpType: OpTypePut, Key: "k", Value: "v"})
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
		_, err := sm.Apply(cmd)
		return err
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
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "ok", Value: "yes"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 2, OpType: OpTypePut, Key: "drop", Value: "me"}); err != nil {
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

func TestWALBadCRCOnFinalRecordTruncates(t *testing.T) {
	for _, extra := range [][]byte{nil, {0, 0, 0, 0}} {
		t.Run(fmt.Sprintf("extra_%d", len(extra)), func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, walFileName)

			w, err := Open(dir, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "ok", Value: "1"}); err != nil {
				t.Fatal(err)
			}
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
			info, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			firstSize := info.Size()
			if err := w.Append(Record{Index: 2, OpType: OpTypePut, Key: "tail", Value: "2"}); err != nil {
				t.Fatal(err)
			}
			if err := w.Sync(); err != nil {
				t.Fatal(err)
			}
			if err := w.Close(); err != nil {
				t.Fatal(err)
			}

			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			data[len(data)-1] ^= 0xff
			data = append(data, extra...)
			if err := os.WriteFile(path, data, 0o644); err != nil {
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

			if len(got) != 1 || got[0].Key != "ok" || got[0].Value != "1" {
				t.Fatalf("after bad crc tail: got %+v, want one put ok", got)
			}
			info, err = os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if info.Size() != firstSize {
				t.Fatalf("wal size = %d, want %d", info.Size(), firstSize)
			}
		})
	}
}

func TestWALBadCRCFollowedByValidRecordFails(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, walFileName)

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	firstSize := info.Size()
	if err := w.Append(Record{Index: 2, OpType: OpTypePut, Key: "b", Value: "2"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[firstSize-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	before := int64(len(data))

	_, err = Open(dir, func(Record) error { return nil })
	if err == nil {
		t.Fatal("Open crc mismatch with a later valid record: err = nil, want error")
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != before {
		t.Fatalf("wal size = %d, want %d", info.Size(), before)
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
			if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "ok", Value: "1"}); err != nil {
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

func TestReplayRejectsIndexGap(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 2, OpType: OpTypePut, Key: "a", Value: "1"}); err == nil {
		t.Fatal("Append index 2 on empty wal: err = nil, want error")
	}
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	appendEncoded(t, dir, Record{Index: 3, OpType: OpTypePut, Key: "c", Value: "3"})
	expectReplayRejected(t, dir)
}

func TestReplayRejectsRepeatedIndex(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	appendEncoded(t, dir, Record{Index: 1, OpType: OpTypePut, Key: "again", Value: "1"})
	expectReplayRejected(t, dir)
}

func TestReplayRejectsUnknownVersion(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	frame, err := Record{Index: 2, OpType: OpTypePut, Key: "b", Value: "2"}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	frame[8] = 9
	payloadLen := binary.LittleEndian.Uint32(frame[4:8])
	payload := frame[8 : 8+payloadLen]
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, payloadLen)
	binary.LittleEndian.PutUint32(frame[0:4], crc32.ChecksumIEEE(append(lengthBuf, payload...)))

	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(frame); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	expectReplayRejected(t, dir)
}

func TestWALOpenAfterSkipsCoveredPrefix(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	recs := []Record{
		{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"},
		{Index: 2, OpType: OpTypePut, Key: "b", Value: "2"},
		{Index: 3, OpType: OpTypePut, Key: "c", Value: "3"},
	}
	if err := w.AppendMany(recs); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	w, err = OpenAfter(dir, 2, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Index != 3 || got[0].Key != "c" {
		t.Fatalf("replay = %+v, want record 3", got)
	}
	if w.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", w.LastIndex())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	got = nil
	w, err = OpenAfter(dir, 2, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(got) != 1 || got[0].Index != 3 {
		t.Fatalf("replay after compact = %+v, want record 3", got)
	}
}

func TestWALOpenAfterEmptyKeepsBase(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	w, err = OpenAfter(dir, 5, func(Record) error {
		t.Fatal("apply on empty log")
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5", w.LastIndex())
	}
	if err := w.Append(Record{Index: 6, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALOpenAfterTruncatesWhenBehindSnapshot(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var applied int
	w, err = OpenAfter(dir, 5, func(Record) error {
		applied++
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("applied = %d, want 0", applied)
	}
	if w.LastIndex() != 5 {
		t.Fatalf("LastIndex = %d, want 5", w.LastIndex())
	}
	info, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("wal size = %d, want 0", info.Size())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALDiscardThroughKeepsTail(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendMany([]Record{
		{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"},
		{Index: 2, OpType: OpTypePut, Key: "b", Value: "2"},
		{Index: 3, OpType: OpTypePut, Key: "c", Value: "3"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.DiscardThrough(2); err != nil {
		t.Fatal(err)
	}
	if w.LastIndex() != 3 {
		t.Fatalf("LastIndex = %d, want 3", w.LastIndex())
	}
	if err := w.Append(Record{Index: 4, OpType: OpTypePut, Key: "d", Value: "4"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	w, err = OpenAfter(dir, 2, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(got) != 2 || got[0].Index != 3 || got[1].Index != 4 {
		t.Fatalf("replay = %+v, want records 3 and 4", got)
	}
}

func TestWALDiscardThroughTwice(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.AppendMany([]Record{
		{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"},
		{Index: 2, OpType: OpTypePut, Key: "b", Value: "2"},
		{Index: 3, OpType: OpTypePut, Key: "c", Value: "3"},
		{Index: 4, OpType: OpTypePut, Key: "d", Value: "4"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := w.DiscardThrough(2); err != nil {
		t.Fatal(err)
	}
	if err := w.DiscardThrough(3); err != nil {
		t.Fatal(err)
	}
	if w.LastIndex() != 4 {
		t.Fatalf("LastIndex = %d, want 4", w.LastIndex())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	var got []Record
	w, err = OpenAfter(dir, 3, func(r Record) error {
		got = append(got, r)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(got) != 1 || got[0].Index != 4 {
		t.Fatalf("replay = %+v, want record 4", got)
	}
}

func TestWALDiscardThroughZeroLeavesFile(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(Record{Index: 1, OpType: OpTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if err := w.DiscardThrough(0); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(filepath.Join(dir, walFileName))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != info.Size() {
		t.Fatalf("wal size = %d, want %d", after.Size(), info.Size())
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestWALOpenAfterRejectsGap(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	appendEncoded(t, dir, Record{Index: 4, OpType: OpTypePut, Key: "a", Value: "1"})
	if _, err := OpenAfter(dir, 2, func(Record) error { return nil }); err == nil {
		t.Fatal("OpenAfter gap: err = nil, want error")
	}
}

func appendEncoded(t *testing.T, dir string, recs ...Record) {
	t.Helper()
	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for _, rec := range recs {
		frame, err := rec.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := f.Write(frame); err != nil {
			t.Fatal(err)
		}
	}
}

func expectReplayRejected(t *testing.T, dir string) {
	t.Helper()
	path := filepath.Join(dir, walFileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	before := info.Size()

	_, err = Open(dir, func(Record) error { return nil })
	if err == nil {
		t.Fatal("Open: err = nil, want error")
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != before {
		t.Fatalf("wal size = %d, want %d", info.Size(), before)
	}
}
