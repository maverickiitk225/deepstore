package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	DefaultSnapshotThreshold uint64 = 1024

	applyTimeout = 5 * time.Second
)

// Config opens one voter. A later Open of the same directory reuses the log
// and does not bootstrap again. NodeID has to stay the same for that directory.
type Config struct {
	Dir               string
	NodeID            string
	RaftAddr          string
	SnapshotThreshold uint64 // 0 disables automatic snapshots
	LogOutput         io.Writer
	// Raft, when set, supplies timeouts and the logger. LocalID and
	// SnapshotThreshold are overwritten from this config.
	Raft *raft.Config
}

type Node struct {
	fsm       *fsm
	raft      *raft.Raft
	transport *raft.NetworkTransport
	logs      *raftboltdb.BoltStore
	stable    *raftboltdb.BoltStore
	closeOnce sync.Once
	closeErr  error
}

func Open(cfg Config) (*Node, error) {
	if cfg.Dir == "" || cfg.NodeID == "" || cfg.RaftAddr == "" {
		return nil, fmt.Errorf("node: dir, node id, and raft address are required")
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, fmt.Errorf("node: mkdir: %w", err)
	}
	logOutput := cfg.LogOutput
	if logOutput == nil {
		logOutput = os.Stderr
	}

	logs, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, "raft-log.db"))
	if err != nil {
		return nil, fmt.Errorf("node: log store: %w", err)
	}
	stable, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, "raft-stable.db"))
	if err != nil {
		_ = logs.Close()
		return nil, fmt.Errorf("node: stable store: %w", err)
	}
	snapDir := filepath.Join(cfg.Dir, "raft-snap")
	snaps, err := raft.NewFileSnapshotStore(snapDir, 2, logOutput)
	if err != nil {
		_ = stable.Close()
		_ = logs.Close()
		return nil, fmt.Errorf("node: snapshot store: %w", err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddr, nil, 3, 10*time.Second, logOutput)
	if err != nil {
		_ = stable.Close()
		_ = logs.Close()
		return nil, fmt.Errorf("node: transport: %w", err)
	}

	has, err := raft.HasExistingState(logs, stable, snaps)
	if err != nil {
		_ = transport.Close()
		_ = stable.Close()
		_ = logs.Close()
		return nil, fmt.Errorf("node: existing state: %w", err)
	}

	rc := cfg.Raft
	if rc == nil {
		rc = raft.DefaultConfig()
		// The threshold is checked on this interval. Keep it short so
		// -snapshot-every takes effect without waiting on the library default.
		rc.SnapshotInterval = 200 * time.Millisecond
	}
	rc.LocalID = raft.ServerID(cfg.NodeID)
	if cfg.SnapshotThreshold == 0 {
		rc.SnapshotThreshold = ^uint64(0)
	} else {
		rc.SnapshotThreshold = cfg.SnapshotThreshold
	}
	if rc.Logger == nil {
		rc.Logger = hclog.New(&hclog.LoggerOptions{
			Name:   "raft",
			Level:  hclog.Info,
			Output: logOutput,
		})
	}

	fsm := newFSM()
	r, err := raft.NewRaft(rc, fsm, logs, stable, snaps, transport)
	if err != nil {
		_ = transport.Close()
		_ = stable.Close()
		_ = logs.Close()
		return nil, fmt.Errorf("node: raft: %w", err)
	}
	n := &Node{fsm: fsm, raft: r, transport: transport, logs: logs, stable: stable}
	if has {
		return n, nil
	}
	fut := r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{{
			ID:       rc.LocalID,
			Address:  transport.LocalAddr(),
			Suffrage: raft.Voter,
		}},
	})
	if err := fut.Error(); err != nil {
		_ = n.Close()
		return nil, fmt.Errorf("node: bootstrap: %w", err)
	}
	return n, nil
}

func (n *Node) RaftAddr() string {
	return string(n.transport.LocalAddr())
}

// WaitForLeader blocks until this node is the leader and a barrier has
// applied, or ctx ends.
func (n *Node) WaitForLeader(ctx context.Context) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if n.raft.State() == raft.Leader {
			if err := n.raft.Barrier(applyTimeout).Error(); err == nil {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("node: wait for leader: %w", ctx.Err())
		case <-ticker.C:
		}
	}
}

func (n *Node) Put(key, value string, clientID, seq uint64) (uint64, error) {
	index, _, err := n.propose(store.Command{
		Type:     store.CommandTypePut,
		Key:      key,
		Value:    value,
		ClientID: clientID,
		Seq:      seq,
	})
	return index, err
}

func (n *Node) Delete(key string, clientID, seq uint64) (uint64, error) {
	index, _, err := n.propose(store.Command{
		Type:     store.CommandTypeDelete,
		Key:      key,
		ClientID: clientID,
		Seq:      seq,
	})
	return index, err
}

func (n *Node) CAS(key, expected, value string, clientID, seq uint64) (uint64, bool, error) {
	return n.propose(store.Command{
		Type:     store.CommandTypeCAS,
		Key:      key,
		Expected: expected,
		Value:    value,
		ClientID: clientID,
		Seq:      seq,
	})
}

func (n *Node) Get(key string) (string, bool, error) {
	if n.raft.State() != raft.Leader {
		return "", false, fmt.Errorf("node: %w", errs.ErrNotLeader)
	}
	if err := n.raft.Barrier(applyTimeout).Error(); err != nil {
		return "", false, leaderErr(err)
	}
	value, ok := n.fsm.sm.Get(key)
	return value, ok, nil
}

func (n *Node) propose(cmd store.Command) (uint64, bool, error) {
	if cmd.Key == "" {
		return 0, false, fmt.Errorf("node: empty key")
	}
	if cmd.ClientID == 0 || cmd.Seq == 0 {
		return 0, false, fmt.Errorf("node: %w: client id and seq are required", errs.ErrSession)
	}
	if sess, ok := n.session(cmd.ClientID); ok && sess.Seq == cmd.Seq {
		return sess.Index, sess.Swapped, nil
	}
	if n.raft.State() != raft.Leader {
		return 0, false, fmt.Errorf("node: %w", errs.ErrNotLeader)
	}
	if err := n.checkNext(cmd); err != nil {
		return 0, false, err
	}
	payload, err := encodeCommand(cmd)
	if err != nil {
		return 0, false, err
	}
	fut := n.raft.Apply(payload, applyTimeout)
	if err := fut.Error(); err != nil {
		return 0, false, leaderErr(err)
	}
	res, ok := fut.Response().(applyResult)
	if !ok {
		return 0, false, fmt.Errorf("node: unexpected apply response %T", fut.Response())
	}
	if res.err != nil {
		return 0, false, res.err
	}
	return res.index, res.swapped, nil
}

func (n *Node) checkNext(cmd store.Command) error {
	sess, ok := n.session(cmd.ClientID)
	want := uint64(1)
	if ok {
		if sess.Seq == cmd.Seq {
			return nil
		}
		want = sess.Seq + 1
	}
	if cmd.Seq != want {
		return fmt.Errorf("node: %w: client %d seq %d, want %d", errs.ErrSession, cmd.ClientID, cmd.Seq, want)
	}
	return nil
}

func (n *Node) session(id uint64) (store.Session, bool) {
	sess, ok := n.fsm.sm.Sessions()[id]
	return sess, ok
}

func leaderErr(err error) error {
	if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
		return fmt.Errorf("node: %w", errs.ErrNotLeader)
	}
	return err
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		var err error
		if n.raft != nil {
			err = n.raft.Shutdown().Error()
		}
		if n.transport != nil {
			err = errors.Join(err, n.transport.Close())
		}
		if n.logs != nil {
			err = errors.Join(err, n.logs.Close())
		}
		if n.stable != nil {
			err = errors.Join(err, n.stable.Close())
		}
		n.closeErr = err
	})
	return n.closeErr
}
