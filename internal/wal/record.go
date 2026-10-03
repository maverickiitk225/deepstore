package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

var errCRCMismatch = errors.New("wal: crc mismatch")

type OpType string

const (
	OpTypePut    OpType = "put"
	OpTypeDelete OpType = "delete"
	OpTypeCAS    OpType = "cas"
)

const (
	opPut    byte = 1
	opDelete byte = 2
	opCAS    byte = 3
)

const (
	// formatV2 is a record with no client session. Replay still accepts it.
	formatV2 byte = 2
	// formatV3 adds client id and sequence after the index.
	formatV3 byte = 3
)

const maxPayloadSize uint32 = 16 << 20

type Record struct {
	Index    uint64
	ClientID uint64
	Seq      uint64
	OpType   OpType
	Key      string
	Value    string
	Expected string // compared by CAS; ignored for put and delete
}

func (r Record) Encode() ([]byte, error) {
	if r.Key == "" {
		return nil, fmt.Errorf("wal: empty key")
	}

	op, err := encodeOp(r.OpType)
	if err != nil {
		return nil, err
	}
	if r.Index == 0 {
		return nil, fmt.Errorf("wal: index must be positive")
	}

	version, err := r.version()
	if err != nil {
		return nil, err
	}
	if r.OpType != OpTypeCAS && r.Expected != "" {
		return nil, fmt.Errorf("wal: expected set on %s", r.OpType)
	}

	key := []byte(r.Key)
	value := []byte(r.Value)
	if r.OpType == OpTypeDelete {
		value = nil
	}
	var expected []byte
	if r.OpType == OpTypeCAS {
		expected = []byte(r.Expected)
	}

	if len(key) > int(^uint32(0)) {
		return nil, fmt.Errorf("wal: key too long")
	}
	if len(value) > int(^uint32(0)) {
		return nil, fmt.Errorf("wal: value too long")
	}
	if len(expected) > int(^uint32(0)) {
		return nil, fmt.Errorf("wal: expected too long")
	}

	payloadLen := 1 + 8 + 1 + 4 + len(key) + 4 + len(value)
	if version == formatV3 {
		payloadLen += 16
	}
	if r.OpType == OpTypeCAS {
		payloadLen += 4 + len(expected)
	}
	if payloadLen > int(maxPayloadSize) {
		return nil, fmt.Errorf("wal: payload too long")
	}
	payload := make([]byte, payloadLen)
	payload[0] = version
	binary.LittleEndian.PutUint64(payload[1:9], r.Index)
	off := 9
	if version == formatV3 {
		binary.LittleEndian.PutUint64(payload[off:], r.ClientID)
		off += 8
		binary.LittleEndian.PutUint64(payload[off:], r.Seq)
		off += 8
	}
	payload[off] = op
	off++
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(key)))
	off += 4
	copy(payload[off:], key)
	off += len(key)
	binary.LittleEndian.PutUint32(payload[off:], uint32(len(value)))
	off += 4
	copy(payload[off:], value)
	if r.OpType == OpTypeCAS {
		off += len(value)
		binary.LittleEndian.PutUint32(payload[off:], uint32(len(expected)))
		off += 4
		copy(payload[off:], expected)
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

func (r *Record) Decode(data []byte) error {
	if len(data) < 8 {
		return fmt.Errorf("wal: frame too short")
	}

	storedCRC := binary.LittleEndian.Uint32(data[0:4])
	payloadLen := binary.LittleEndian.Uint32(data[4:8])
	if payloadLen > maxPayloadSize {
		return fmt.Errorf("wal: payload length %d exceeds max %d", payloadLen, maxPayloadSize)
	}
	if int(payloadLen) > len(data)-8 {
		return fmt.Errorf("wal: length exceeds frame")
	}

	payload := data[8 : 8+payloadLen]
	lengthBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lengthBuf, payloadLen)
	if crc32.ChecksumIEEE(append(lengthBuf, payload...)) != storedCRC {
		return errCRCMismatch
	}

	if len(payload) < 1 {
		return fmt.Errorf("wal: payload too short")
	}
	version := payload[0]
	if version != formatV2 && version != formatV3 {
		return fmt.Errorf("wal: unknown version: %d", version)
	}
	header := 1 + 8 + 1 + 4 + 4
	if version == formatV3 {
		header += 16
	}
	if len(payload) < header {
		return fmt.Errorf("wal: payload too short")
	}

	index := binary.LittleEndian.Uint64(payload[1:9])
	off := 9
	var clientID, seq uint64
	if version == formatV3 {
		clientID = binary.LittleEndian.Uint64(payload[off:])
		off += 8
		seq = binary.LittleEndian.Uint64(payload[off:])
		off += 8
		if clientID == 0 || seq == 0 {
			return fmt.Errorf("wal: client id and seq are required")
		}
	}
	op := payload[off]
	off++
	keyLen := binary.LittleEndian.Uint32(payload[off:])
	off += 4
	if keyLen == 0 {
		return fmt.Errorf("wal: empty key")
	}
	if int(keyLen) > len(payload)-off {
		return fmt.Errorf("wal: invalid key length")
	}
	key := make([]byte, keyLen)
	copy(key, payload[off:off+int(keyLen)])
	off += int(keyLen)

	if len(payload)-off < 4 {
		return fmt.Errorf("wal: missing value length")
	}
	valLen := binary.LittleEndian.Uint32(payload[off:])
	off += 4
	if int(valLen) > len(payload)-off {
		return fmt.Errorf("wal: invalid value length")
	}
	value := make([]byte, valLen)
	copy(value, payload[off:off+int(valLen)])
	off += int(valLen)

	opType, err := decodeOp(op)
	if err != nil {
		return err
	}

	var expected []byte
	if opType == OpTypeCAS {
		if len(payload)-off < 4 {
			return fmt.Errorf("wal: missing expected length")
		}
		expLen := binary.LittleEndian.Uint32(payload[off:])
		off += 4
		if int(expLen) > len(payload)-off {
			return fmt.Errorf("wal: invalid expected length")
		}
		expected = make([]byte, expLen)
		copy(expected, payload[off:off+int(expLen)])
		off += int(expLen)
	}
	if off != len(payload) {
		return fmt.Errorf("wal: trailing payload bytes")
	}

	r.Index = index
	r.ClientID = clientID
	r.Seq = seq
	r.OpType = opType
	r.Key = string(key)
	r.Value = string(value)
	r.Expected = string(expected)
	return nil
}

func (r Record) version() (byte, error) {
	if r.ClientID == 0 && r.Seq == 0 {
		return formatV2, nil
	}
	if r.ClientID == 0 || r.Seq == 0 {
		return 0, fmt.Errorf("wal: client id and seq are required")
	}
	return formatV3, nil
}

func encodeOp(t OpType) (byte, error) {
	switch t {
	case OpTypePut:
		return opPut, nil
	case OpTypeDelete:
		return opDelete, nil
	case OpTypeCAS:
		return opCAS, nil
	default:
		return 0, fmt.Errorf("wal: op not encodable: %q", t)
	}
}

func decodeOp(op byte) (OpType, error) {
	switch op {
	case opPut:
		return OpTypePut, nil
	case opDelete:
		return OpTypeDelete, nil
	case opCAS:
		return OpTypeCAS, nil
	default:
		return "", fmt.Errorf("wal: unknown op: %d", op)
	}
}
