package convert

import (
	"encoding/binary"
	"fmt"
	"slices"
)

func EncodePeers(peers map[string]string) ([]byte, error) {
	ids := make([]string, 0, len(peers))
	for id, addr := range peers {
		if id == "" || addr == "" {
			return nil, fmt.Errorf("convert: empty peer")
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)
	n := 4
	for _, id := range ids {
		n += 4 + len(id) + 4 + len(peers[id])
	}
	buf := make([]byte, n)
	binary.LittleEndian.PutUint32(buf, uint32(len(ids)))
	off := 4
	for _, id := range ids {
		off = putBytes(buf, off, []byte(id))
		off = putBytes(buf, off, []byte(peers[id]))
	}
	return buf, nil
}

func DecodePeers(data []byte) (map[string]string, error) {
	if len(data) == 0 {
		return map[string]string{}, nil
	}
	if len(data) < 4 {
		return nil, fmt.Errorf("convert: peer list too short")
	}
	n := binary.LittleEndian.Uint32(data[:4])
	if n > 1024 {
		return nil, fmt.Errorf("convert: peer list %d is too large", n)
	}
	off := 4
	out := make(map[string]string, n)
	for i := uint32(0); i < n; i++ {
		id, next, err := takeBytes(data, off)
		if err != nil {
			return nil, err
		}
		addr, next, err := takeBytes(data, next)
		if err != nil {
			return nil, err
		}
		off = next
		if len(id) == 0 || len(addr) == 0 {
			return nil, fmt.Errorf("convert: empty peer")
		}
		s := string(id)
		if _, ok := out[s]; ok {
			return nil, fmt.Errorf("convert: duplicate peer %s", s)
		}
		out[s] = string(addr)
	}
	if off != len(data) {
		return nil, fmt.Errorf("convert: trailing peer bytes")
	}
	return out, nil
}
