package node

import (
	"encoding/binary"
	"fmt"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/store"
)

const (
	cmdVersion byte = 1
	opPut      byte = 1
	opDelete   byte = 2
	opCAS      byte = 3
)

const maxCommandSize = 16 << 20

// encodeCommand writes a command without a log index. The Raft log assigns
// the index, and Apply copies it onto the command before the state machine
// sees it.
func encodeCommand(cmd store.Command) ([]byte, error) {
	if cmd.Key == "" {
		return nil, fmt.Errorf("node: empty key")
	}
	if cmd.ClientID == 0 || cmd.Seq == 0 {
		return nil, fmt.Errorf("node: %w: client id and seq are required", errs.ErrSession)
	}
	op, err := encodeOp(cmd.Type)
	if err != nil {
		return nil, err
	}
	if cmd.Type != store.CommandTypeCAS && cmd.Expected != "" {
		return nil, fmt.Errorf("node: expected set on %s", cmd.Type)
	}

	key := []byte(cmd.Key)
	value := []byte(cmd.Value)
	if cmd.Type == store.CommandTypeDelete {
		value = nil
	}
	var expected []byte
	if cmd.Type == store.CommandTypeCAS {
		expected = []byte(cmd.Expected)
	}
	if len(key) > int(^uint32(0)) || len(value) > int(^uint32(0)) || len(expected) > int(^uint32(0)) {
		return nil, fmt.Errorf("node: command too large")
	}
	n := 1 + 1 + 8 + 8 + 4 + len(key) + 4 + len(value) + 4 + len(expected)
	if n > maxCommandSize {
		return nil, fmt.Errorf("node: command too large")
	}

	buf := make([]byte, n)
	buf[0] = cmdVersion
	buf[1] = op
	binary.LittleEndian.PutUint64(buf[2:10], cmd.ClientID)
	binary.LittleEndian.PutUint64(buf[10:18], cmd.Seq)
	off := 18
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(key)))
	off += 4
	copy(buf[off:], key)
	off += len(key)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(value)))
	off += 4
	copy(buf[off:], value)
	off += len(value)
	binary.LittleEndian.PutUint32(buf[off:], uint32(len(expected)))
	off += 4
	copy(buf[off:], expected)
	return buf, nil
}

func decodeCommand(data []byte) (store.Command, error) {
	if len(data) > maxCommandSize {
		return store.Command{}, fmt.Errorf("node: command too large")
	}
	if len(data) < 18+4 {
		return store.Command{}, fmt.Errorf("node: command too short")
	}
	if data[0] != cmdVersion {
		return store.Command{}, fmt.Errorf("node: unknown command version %d", data[0])
	}
	op := data[1]
	cmd := store.Command{
		ClientID: binary.LittleEndian.Uint64(data[2:10]),
		Seq:      binary.LittleEndian.Uint64(data[10:18]),
	}
	var err error
	cmd.Type, err = decodeOp(op)
	if err != nil {
		return store.Command{}, err
	}
	if cmd.ClientID == 0 || cmd.Seq == 0 {
		return store.Command{}, fmt.Errorf("node: %w: client id and seq are required", errs.ErrSession)
	}
	off := 18
	key, off, err := takeBytes(data, off)
	if err != nil {
		return store.Command{}, err
	}
	if len(key) == 0 {
		return store.Command{}, fmt.Errorf("node: empty key")
	}
	value, off, err := takeBytes(data, off)
	if err != nil {
		return store.Command{}, err
	}
	expected, off, err := takeBytes(data, off)
	if err != nil {
		return store.Command{}, err
	}
	if off != len(data) {
		return store.Command{}, fmt.Errorf("node: trailing command bytes")
	}
	if cmd.Type == store.CommandTypeDelete {
		value = nil
	}
	if cmd.Type != store.CommandTypeCAS && len(expected) != 0 {
		return store.Command{}, fmt.Errorf("node: expected set on %s", cmd.Type)
	}
	cmd.Key = string(key)
	cmd.Value = string(value)
	cmd.Expected = string(expected)
	return cmd, nil
}

func takeBytes(data []byte, off int) ([]byte, int, error) {
	if len(data)-off < 4 {
		return nil, 0, fmt.Errorf("node: command too short")
	}
	n := binary.LittleEndian.Uint32(data[off:])
	off += 4
	if int(n) > len(data)-off {
		return nil, 0, fmt.Errorf("node: command length exceeds payload")
	}
	buf := make([]byte, n)
	copy(buf, data[off:off+int(n)])
	return buf, off + int(n), nil
}

func encodeOp(t store.CommandType) (byte, error) {
	switch t {
	case store.CommandTypePut:
		return opPut, nil
	case store.CommandTypeDelete:
		return opDelete, nil
	case store.CommandTypeCAS:
		return opCAS, nil
	default:
		return 0, fmt.Errorf("node: unsupported command %q", t)
	}
}

func decodeOp(op byte) (store.CommandType, error) {
	switch op {
	case opPut:
		return store.CommandTypePut, nil
	case opDelete:
		return store.CommandTypeDelete, nil
	case opCAS:
		return store.CommandTypeCAS, nil
	default:
		return "", fmt.Errorf("node: unsupported op %d", op)
	}
}
