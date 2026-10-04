package snapshot

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
)

const (
	fileName         = "snapshot"
	tmpFileName      = "snapshot.tmp"
	formatV1    byte = 1
)

const maxSnapshotSize uint32 = 1 << 30

type Session struct {
	Seq     uint64
	Index   uint64
	Swapped bool
}

type Snapshot struct {
	Index    uint64
	Data     map[string]string
	Sessions map[uint64]Session
}

func Save(dir string, snap Snapshot) error {
	frame, err := encode(snap)
	if err != nil {
		return err
	}
	tmp := filepath.Join(dir, tmpFileName)
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("snapshot: create: %w", err)
	}
	n, err := f.Write(frame)
	if n != len(frame) {
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("snapshot: write: %w", err)
		}
		return fmt.Errorf("snapshot: short write: wrote %d of %d bytes", n, len(frame))
	}
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: write: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("snapshot: sync: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("snapshot: close: %w", err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, fileName)); err != nil {
		return fmt.Errorf("snapshot: rename: %w", err)
	}
	return syncDir(dir)
}

func Load(dir string) (Snapshot, bool, error) {
	data, err := os.ReadFile(filepath.Join(dir, fileName))
	if err != nil {
		if os.IsNotExist(err) {
			return Snapshot{}, false, nil
		}
		return Snapshot{}, false, fmt.Errorf("snapshot: read: %w", err)
	}
	snap, err := decode(data)
	if err != nil {
		return Snapshot{}, false, err
	}
	return snap, true, nil
}

func encode(snap Snapshot) ([]byte, error) {
	if err := validate(snap); err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(snap.Data))
	for k := range snap.Data {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	ids := make([]uint64, 0, len(snap.Sessions))
	for id := range snap.Sessions {
		ids = append(ids, id)
	}
	slices.Sort(ids)

	n := 1 + 8 + 4
	var err error
	for _, k := range keys {
		n, err = grow(n, 4+len(k)+4+len(snap.Data[k]))
		if err != nil {
			return nil, err
		}
	}
	n, err = grow(n, 4+len(ids)*(8+8+8+1))
	if err != nil {
		return nil, err
	}

	payload := make([]byte, n)
	payload[0] = formatV1
	binary.LittleEndian.PutUint64(payload[1:9], snap.Index)
	binary.LittleEndian.PutUint32(payload[9:13], uint32(len(keys)))
	off := 13
	for _, k := range keys {
		v := snap.Data[k]
		binary.LittleEndian.PutUint32(payload[off:], uint32(len(k)))
		off += 4
		copy(payload[off:], k)
		off += len(k)
		binary.LittleEndian.PutUint32(payload[off:], uint32(len(v)))
		off += 4
		copy(payload[off:], v)
		off += len(v)
	}
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(ids)))
	off += 4
	for _, id := range ids {
		sess := snap.Sessions[id]
		binary.LittleEndian.PutUint64(payload[off:], id)
		off += 8
		binary.LittleEndian.PutUint64(payload[off:], sess.Seq)
		off += 8
		binary.LittleEndian.PutUint64(payload[off:], sess.Index)
		off += 8
		if sess.Swapped {
			payload[off] = 1
		}
		off++
	}
	if off != len(payload) {
		return nil, fmt.Errorf("snapshot: encode size %d, wrote %d", len(payload), off)
	}

	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, uint32(len(payload)))
	crc := crc32.ChecksumIEEE(append(lengthBuf, payload...))
	frame := make([]byte, 8+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], crc)
	binary.LittleEndian.PutUint32(frame[4:8], uint32(len(payload)))
	copy(frame[8:], payload)
	return frame, nil
}

func decode(data []byte) (Snapshot, error) {
	if len(data) < 8 {
		return Snapshot{}, fmt.Errorf("snapshot: frame too short")
	}
	storedCRC := binary.LittleEndian.Uint32(data[0:4])
	payloadLen := binary.LittleEndian.Uint32(data[4:8])
	if payloadLen > maxSnapshotSize {
		return Snapshot{}, fmt.Errorf("snapshot: payload length %d exceeds max %d", payloadLen, maxSnapshotSize)
	}
	if uint64(len(data)) != 8+uint64(payloadLen) {
		return Snapshot{}, fmt.Errorf("snapshot: length mismatch")
	}
	payload := data[8:]
	if crc32.ChecksumIEEE(data[4:]) != storedCRC {
		return Snapshot{}, fmt.Errorf("snapshot: crc mismatch")
	}
	if len(payload) < 1+8+4+4 {
		return Snapshot{}, fmt.Errorf("snapshot: payload too short")
	}
	version := payload[0]
	if version != formatV1 {
		return Snapshot{}, fmt.Errorf("snapshot: unknown version: %d", version)
	}
	index := binary.LittleEndian.Uint64(payload[1:9])
	nkeys := binary.LittleEndian.Uint32(payload[9:13])
	off := 13
	if int(nkeys) > (len(payload)-off)/9 {
		return Snapshot{}, fmt.Errorf("snapshot: key count %d exceeds payload", nkeys)
	}
	out := make(map[string]string, nkeys)
	for i := uint32(0); i < nkeys; i++ {
		if len(payload)-off < 4 {
			return Snapshot{}, fmt.Errorf("snapshot: missing key length")
		}
		keyLen := binary.LittleEndian.Uint32(payload[off:])
		off += 4
		if keyLen == 0 {
			return Snapshot{}, fmt.Errorf("snapshot: empty key")
		}
		if int(keyLen) > len(payload)-off {
			return Snapshot{}, fmt.Errorf("snapshot: invalid key length")
		}
		key := make([]byte, keyLen)
		copy(key, payload[off:off+int(keyLen)])
		off += int(keyLen)
		if len(payload)-off < 4 {
			return Snapshot{}, fmt.Errorf("snapshot: missing value length")
		}
		valLen := binary.LittleEndian.Uint32(payload[off:])
		off += 4
		if int(valLen) > len(payload)-off {
			return Snapshot{}, fmt.Errorf("snapshot: invalid value length")
		}
		value := make([]byte, valLen)
		copy(value, payload[off:off+int(valLen)])
		off += int(valLen)
		ks := string(key)
		if _, ok := out[ks]; ok {
			return Snapshot{}, fmt.Errorf("snapshot: duplicate key %q", ks)
		}
		out[ks] = string(value)
	}
	if len(payload)-off < 4 {
		return Snapshot{}, fmt.Errorf("snapshot: missing session count")
	}
	nsess := binary.LittleEndian.Uint32(payload[off:])
	off += 4
	if int(nsess) > (len(payload)-off)/25 {
		return Snapshot{}, fmt.Errorf("snapshot: session count %d exceeds payload", nsess)
	}
	sessions := make(map[uint64]Session, nsess)
	for i := uint32(0); i < nsess; i++ {
		if len(payload)-off < 25 {
			return Snapshot{}, fmt.Errorf("snapshot: session too short")
		}
		id := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		seq := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		sessIndex := binary.LittleEndian.Uint64(payload[off:])
		off += 8
		swapped := payload[off]
		off++
		if swapped > 1 {
			return Snapshot{}, fmt.Errorf("snapshot: swapped %d", swapped)
		}
		if id == 0 || seq == 0 {
			return Snapshot{}, fmt.Errorf("snapshot: client id and seq are required")
		}
		if sessIndex == 0 || sessIndex > index {
			return Snapshot{}, fmt.Errorf("snapshot: session index %d outside 1..%d", sessIndex, index)
		}
		if _, ok := sessions[id]; ok {
			return Snapshot{}, fmt.Errorf("snapshot: duplicate client %d", id)
		}
		sessions[id] = Session{Seq: seq, Index: sessIndex, Swapped: swapped == 1}
	}
	if off != len(payload) {
		return Snapshot{}, fmt.Errorf("snapshot: trailing payload bytes")
	}
	snap := Snapshot{Index: index, Data: out, Sessions: sessions}
	if err := validate(snap); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

func validate(snap Snapshot) error {
	if len(snap.Data) > int(^uint32(0)) || len(snap.Sessions) > int(^uint32(0)) {
		return fmt.Errorf("snapshot: too large")
	}
	if snap.Index == 0 && (len(snap.Data) > 0 || len(snap.Sessions) > 0) {
		return fmt.Errorf("snapshot: index 0 with state")
	}
	for k, v := range snap.Data {
		if k == "" {
			return fmt.Errorf("snapshot: empty key")
		}
		if len(k) > int(^uint32(0)) || len(v) > int(^uint32(0)) {
			return fmt.Errorf("snapshot: entry too large")
		}
	}
	for id, sess := range snap.Sessions {
		if id == 0 || sess.Seq == 0 {
			return fmt.Errorf("snapshot: client id and seq are required")
		}
		if sess.Index == 0 || sess.Index > snap.Index {
			return fmt.Errorf("snapshot: session index %d outside 1..%d", sess.Index, snap.Index)
		}
	}
	return nil
}

func grow(n, add int) (int, error) {
	if add < 0 || n > int(maxSnapshotSize)-add {
		return 0, fmt.Errorf("snapshot: too large")
	}
	return n + add, nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("snapshot: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("snapshot: sync dir: %w", err)
	}
	return nil
}
