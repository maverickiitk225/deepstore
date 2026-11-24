package node

import (
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"sync"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/node/convert"
	"github.com/deepanker/deepstore/internal/snapshot"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/hashicorp/raft"
)

type applyResult struct {
	index   uint64
	swapped bool
	err     error
}

type fsm struct {
	sm    *store.StateMachine
	mu    sync.RWMutex
	index uint64
	http  map[string]string
}

func newFSM() *fsm {
	return &fsm{sm: store.NewStateMachine(), http: map[string]string{}}
}

func (f *fsm) Apply(l *raft.Log) any {
	if convert.IsAdvertise(l.Data) {
		id, addr, err := convert.DecodeAdvertise(l.Data)
		if err != nil {
			return applyResult{err: err}
		}
		f.mu.Lock()
		f.http[id] = addr
		f.index = l.Index
		f.mu.Unlock()
		return applyResult{index: l.Index}
	}
	cmd, err := convert.DecodeCommand(l.Data)
	if err != nil {
		return applyResult{err: err}
	}
	cmd.Index = l.Index
	res, err := f.sm.Apply(cmd)
	f.mu.Lock()
	f.index = l.Index
	f.mu.Unlock()
	if err != nil {
		return applyResult{err: fmt.Errorf("node: %w: %s", errs.ErrSession, err.Error())}
	}
	return applyResult{index: res.Index, swapped: res.Swapped}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	index := f.index
	peers := maps.Clone(f.http)
	f.mu.RUnlock()
	data, sessions := f.sm.Export()
	snapSessions := make(map[uint64]snapshot.Session, len(sessions))
	for id, sess := range sessions {
		snapSessions[id] = snapshot.Session{Seq: sess.Seq, Index: sess.Index, Swapped: sess.Swapped}
	}
	return &fsmSnap{snap: snapshot.Snapshot{
		Index:    index,
		Data:     data,
		Sessions: snapSessions,
	}, peers: peers}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	header := make([]byte, 8)
	if _, err := io.ReadFull(rc, header); err != nil {
		return fmt.Errorf("node: read snapshot: %w", err)
	}
	payloadLen := binary.LittleEndian.Uint32(header[4:8])
	if payloadLen > 1<<30 {
		return fmt.Errorf("node: snapshot payload %d is too large", payloadLen)
	}
	frame := make([]byte, 8+int(payloadLen))
	copy(frame, header)
	if _, err := io.ReadFull(rc, frame[8:]); err != nil {
		return fmt.Errorf("node: read snapshot: %w", err)
	}
	snap, err := snapshot.Decode(frame)
	if err != nil {
		return err
	}
	rest, err := io.ReadAll(io.LimitReader(rc, 1<<20+1))
	if err != nil {
		return fmt.Errorf("node: read snapshot peers: %w", err)
	}
	if len(rest) > 1<<20 {
		return fmt.Errorf("node: snapshot peers are too large")
	}
	peers, err := convert.DecodePeers(rest)
	if err != nil {
		return err
	}
	f.sm.Restore(snap)
	f.mu.Lock()
	f.http = peers
	f.index = snap.Index
	f.mu.Unlock()
	return nil
}

type fsmSnap struct {
	snap  snapshot.Snapshot
	peers map[string]string
}

func (s *fsmSnap) Persist(sink raft.SnapshotSink) error {
	frame, err := snapshot.Encode(s.snap)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	// The HTTP addresses are written after the snapshot.
	// The snapshot's length covers only the keys and sessions, so the addresses are stored separately.
	peers, err := convert.EncodePeers(s.peers)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(frame); err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(peers); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnap) Release() {}
