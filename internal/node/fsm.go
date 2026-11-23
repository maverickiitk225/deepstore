package node

import (
	"fmt"
	"io"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/snapshot"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/hashicorp/raft"
)

const maxSnapshotFrame = int64(1<<30) + 8

type applyResult struct {
	index   uint64
	swapped bool
	err     error
}

type fsm struct {
	sm    *store.StateMachine
	index uint64
}

func newFSM() *fsm {
	return &fsm{sm: store.NewStateMachine()}
}

func (f *fsm) Apply(l *raft.Log) any {
	cmd, err := decodeCommand(l.Data)
	if err != nil {
		return applyResult{err: err}
	}
	cmd.Index = l.Index
	res, err := f.sm.Apply(cmd)
	f.index = l.Index
	if err != nil {
		return applyResult{err: fmt.Errorf("node: %w: %s", errs.ErrSession, err.Error())}
	}
	return applyResult{index: res.Index, swapped: res.Swapped}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	data, sessions := f.sm.Export()
	snapSessions := make(map[uint64]snapshot.Session, len(sessions))
	for id, sess := range sessions {
		snapSessions[id] = snapshot.Session{Seq: sess.Seq, Index: sess.Index, Swapped: sess.Swapped}
	}
	return &fsmSnap{snap: snapshot.Snapshot{
		Index:    f.index,
		Data:     data,
		Sessions: snapSessions,
	}}, nil
}

func (f *fsm) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(io.LimitReader(rc, maxSnapshotFrame))
	if err != nil {
		return fmt.Errorf("node: read snapshot: %w", err)
	}
	snap, err := snapshot.Decode(data)
	if err != nil {
		return err
	}
	f.sm.Restore(snap)
	f.index = snap.Index
	return nil
}

type fsmSnap struct {
	snap snapshot.Snapshot
}

func (s *fsmSnap) Persist(sink raft.SnapshotSink) error {
	frame, err := snapshot.Encode(s.snap)
	if err != nil {
		_ = sink.Cancel()
		return err
	}
	if _, err := sink.Write(frame); err != nil {
		_ = sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnap) Release() {}
