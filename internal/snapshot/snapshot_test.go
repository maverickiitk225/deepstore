package snapshot

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	in := Snapshot{
		Index: 4,
		Data: map[string]string{
			"b":     "2",
			"a":     "",
			"empty": "",
		},
		Sessions: map[uint64]Session{
			9: {Seq: 2, Index: 4, Swapped: true},
			3: {Seq: 1, Index: 2, Swapped: false},
		},
	}
	if err := Save(dir, in); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, tmpFileName)); !os.IsNotExist(err) {
		t.Fatalf("tmp stat err = %v, want not exist", err)
	}

	out, ok, err := Load(dir)
	if err != nil || !ok {
		t.Fatalf("Load = (%v, %v)", ok, err)
	}
	if out.Index != in.Index || len(out.Data) != len(in.Data) || len(out.Sessions) != len(in.Sessions) {
		t.Fatalf("Load = %+v", out)
	}
	for k, v := range in.Data {
		if out.Data[k] != v {
			t.Fatalf("data[%q] = %q, want %q", k, out.Data[k], v)
		}
	}
	for id, sess := range in.Sessions {
		if out.Sessions[id] != sess {
			t.Fatalf("session %d = %+v, want %+v", id, out.Sessions[id], sess)
		}
	}

	frame, err := encode(in)
	if err != nil {
		t.Fatal(err)
	}
	again := Snapshot{
		Index:    in.Index,
		Data:     map[string]string{"empty": "", "a": "", "b": "2"},
		Sessions: map[uint64]Session{3: in.Sessions[3], 9: in.Sessions[9]},
	}
	frame2, err := encode(again)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(frame, frame2) {
		t.Fatal("encode depends on map iteration order")
	}

	decoded, err := decode(frame)
	if err != nil {
		t.Fatal(err)
	}
	frame[len(frame)-1] ^= 0xff
	if decoded.Data["b"] != "2" {
		t.Fatal("decode aliased value bytes")
	}
}

func TestLoadMissing(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, tmpFileName), []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, ok, err := Load(dir)
	if err != nil || ok {
		t.Fatalf("Load missing = (%v, %v), want (false, nil)", ok, err)
	}
}

func TestLoadRejectsShortFile(t *testing.T) {
	dir := t.TempDir()
	in := Snapshot{Index: 1, Data: map[string]string{"k": "v"}}
	if err := Save(dir, in); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fileName)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(dir); err == nil {
		t.Fatal("Load short file: err = nil, want error")
	}
}

func TestLoadRejectsBadCRC(t *testing.T) {
	dir := t.TempDir()
	if err := Save(dir, Snapshot{Index: 1, Data: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, fileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xff
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Load(dir); err == nil {
		t.Fatal("Load bad crc: err = nil, want error")
	}
}

func TestLoadRejectsUnknownVersion(t *testing.T) {
	frame, err := encode(Snapshot{Index: 1, Data: map[string]string{"k": "v"}})
	if err != nil {
		t.Fatal(err)
	}
	frame[8] = 9
	payloadLen := binary.LittleEndian.Uint32(frame[4:8])
	binary.LittleEndian.PutUint32(frame[0:4], crc32.ChecksumIEEE(frame[4:8+payloadLen]))

	if _, err := decode(frame); err == nil {
		t.Fatal("decode unknown version: err = nil, want error")
	}
}

func TestEncodeRejectsBadSession(t *testing.T) {
	_, err := encode(Snapshot{
		Index:    1,
		Sessions: map[uint64]Session{1: {Seq: 1, Index: 2}},
	})
	if err == nil {
		t.Fatal("session past snapshot: err = nil, want error")
	}
	_, err = encode(Snapshot{Index: 0, Data: map[string]string{"k": "v"}})
	if err == nil {
		t.Fatal("index 0 with state: err = nil, want error")
	}
}
