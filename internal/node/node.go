package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/node/convert"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

const (
	DefaultSnapshotThreshold uint64 = 1024

	applyTimeout = 5 * time.Second

	raftLogFile    = "raft-log.db"
	raftStableFile = "raft-stable.db"
	raftSnapDir    = "raft-snap"
)

// Config is the settings for one server.
// Use the same NodeID and RaftAddr each time you open Dir.
// Set Bootstrap to true only on the first server of a new cluster.
// If Dir already has a Raft log, Open reuses that log.
type Config struct {
	Dir       string
	NodeID    string
	RaftAddr  string
	Bootstrap bool
	// SnapshotThreshold is how many log entries to write before saving a snapshot.
	// Zero turns automatic snapshots off.
	SnapshotThreshold uint64
	LogOutput         io.Writer
	// Raft sets timeouts and the logger. Open still fills in LocalID and SnapshotThreshold from this config.
	Raft *raft.Config
}

type Node struct {
	id        string
	existing  bool
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

	logs, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, raftLogFile))
	if err != nil {
		return nil, fmt.Errorf("node: log store: %w", err)
	}
	stable, err := raftboltdb.NewBoltStore(filepath.Join(cfg.Dir, raftStableFile))
	if err != nil {
		_ = logs.Close()
		return nil, fmt.Errorf("node: stable store: %w", err)
	}
	snapDir := filepath.Join(cfg.Dir, raftSnapDir)
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
		// Raft looks for a due snapshot on this timer.
		// The library default is 2 minutes, which is too slow for -snapshot-every.
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
	n := &Node{
		id:        cfg.NodeID,
		existing:  has,
		fsm:       fsm,
		raft:      r,
		transport: transport,
		logs:      logs,
		stable:    stable,
	}
	if has || !cfg.Bootstrap {
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

// HasExistingState reports whether dir already holds a Raft log, term, or snapshot.
// A missing directory does not. Empty store files left by an earlier open do not either.
func HasExistingState(dir string) (bool, error) {
	if dir == "" {
		return false, fmt.Errorf("node: dir is required")
	}
	info, err := os.Stat(dir)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("node: existing state: %w", err)
	}
	if !info.IsDir() {
		return false, fmt.Errorf("node: existing state: %s is not a directory", dir)
	}
	logPath := filepath.Join(dir, raftLogFile)
	stablePath := filepath.Join(dir, raftStableFile)
	snapPath := filepath.Join(dir, raftSnapDir)
	found, err := anyPathExists(logPath, stablePath, snapPath)
	if err != nil {
		return false, fmt.Errorf("node: existing state: %w", err)
	}
	if !found {
		return false, nil
	}
	logs, err := raftboltdb.NewBoltStore(logPath)
	if err != nil {
		return false, fmt.Errorf("node: existing state: %w", err)
	}
	defer logs.Close()
	stable, err := raftboltdb.NewBoltStore(stablePath)
	if err != nil {
		return false, fmt.Errorf("node: existing state: %w", err)
	}
	defer stable.Close()
	snaps, err := raft.NewFileSnapshotStore(snapPath, 2, io.Discard)
	if err != nil {
		return false, fmt.Errorf("node: existing state: %w", err)
	}
	has, err := raft.HasExistingState(logs, stable, snaps)
	if err != nil {
		return false, fmt.Errorf("node: existing state: %w", err)
	}
	return has, nil
}

func anyPathExists(paths ...string) (bool, error) {
	for _, path := range paths {
		_, err := os.Stat(path)
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	return false, nil
}

func (n *Node) RaftAddr() string {
	return string(n.transport.LocalAddr())
}

func (n *Node) HasExistingState() bool {
	return n.existing
}

func (n *Node) VoterCount() int {
	fut := n.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return 0
	}
	count := 0
	for _, server := range fut.Configuration().Servers {
		if server.Suffrage == raft.Voter {
			count++
		}
	}
	return count
}

func (n *Node) LeaderHTTP() string {
	_, id := n.raft.LeaderWithID()
	if id == "" {
		return ""
	}
	return n.httpAddr(string(id))
}

// AddServer adds a server that can vote.
// raftAddr must already be accepting connections.
// httpAddr is the address clients should use for that server.
// This returns after the new server is a voter.
// If id is already used by a different address, it returns errs.ErrConflict.
// With replace set, id is moved to raftAddr instead. This server's own id can't be replaced.
func (n *Node) AddServer(id, raftAddr, httpAddr string, replace bool) error {
	httpAddr, err := normalizeHTTP(httpAddr)
	if err != nil {
		return err
	}
	raftAddr, err = normalizeRaftAddr(raftAddr)
	if err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("node: id is required")
	}
	if id == n.id && raftAddr != n.RaftAddr() {
		return fmt.Errorf("node: %w: id %s is this server at %s and cannot be replaced", errs.ErrConflict, id, n.RaftAddr())
	}
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("node: %w", errs.ErrNotLeader)
	}
	addr, found, err := n.serverAddr(id)
	if err != nil {
		return err
	}
	if found && addr != raftAddr && !replace {
		return fmt.Errorf("node: %w: id %s is already a server at %s", errs.ErrConflict, id, addr)
	}
	already, err := n.isVoter(id, raftAddr)
	if err != nil {
		return err
	}
	if !already {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		fut := n.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(raftAddr), 0, applyTimeout)
		if err := waitFuture(ctx, fut); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("node: add voter %s: timed out", id)
			}
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) {
				return fmt.Errorf("node: %w", errs.ErrNotLeader)
			}
			return fmt.Errorf("node: add voter: %w", err)
		}
		vctx, vcancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer vcancel()
		if err := n.waitVoter(vctx, id, raftAddr); err != nil {
			return err
		}
	}
	return n.recordHTTP(id, httpAddr)
}

func (n *Node) Advertise(httpAddr string) error {
	httpAddr, err := normalizeHTTP(httpAddr)
	if err != nil {
		return err
	}
	return n.recordHTTP(n.id, httpAddr)
}

func (n *Node) recordHTTP(id, httpAddr string) error {
	if n.httpAddr(id) == httpAddr {
		return nil
	}
	if n.raft.State() != raft.Leader {
		return fmt.Errorf("node: %w", errs.ErrNotLeader)
	}
	payload, err := convert.EncodeAdvertise(id, httpAddr)
	if err != nil {
		return err
	}
	fut := n.raft.Apply(payload, applyTimeout)
	if err := fut.Error(); err != nil {
		return leaderErr(err)
	}
	res, ok := fut.Response().(applyResult)
	if !ok {
		return fmt.Errorf("node: unexpected apply response %T", fut.Response())
	}
	return res.err
}

func (n *Node) httpAddr(id string) string {
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	return n.fsm.http[id]
}

func (n *Node) applied() uint64 {
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	return n.fsm.index
}

func (n *Node) isVoter(id, addr string) (bool, error) {
	fut := n.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return false, err
	}
	for _, server := range fut.Configuration().Servers {
		if string(server.ID) == id && string(server.Address) == addr && server.Suffrage == raft.Voter {
			return true, nil
		}
	}
	return false, nil
}

// serverAddr returns the Raft address of id in the configuration.
func (n *Node) serverAddr(id string) (string, bool, error) {
	fut := n.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return "", false, err
	}
	for _, server := range fut.Configuration().Servers {
		if string(server.ID) == id {
			return string(server.Address), true, nil
		}
	}
	return "", false, nil
}

func (n *Node) waitVoter(ctx context.Context, id, addr string) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		ok, err := n.isVoter(id, addr)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("node: wait for voter %s: %w", id, ctx.Err())
		case <-ticker.C:
		}
	}
}

func waitFuture(ctx context.Context, fut raft.Future) error {
	errCh := make(chan error, 1)
	go func() {
		errCh <- fut.Error()
	}()
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func normalizeHTTP(addr string) (string, error) {
	addr = strings.TrimSpace(addr)
	if addr == "" {
		return "", fmt.Errorf("node: http address is required")
	}
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("node: http address %q", addr)
	}
	u.Path = ""
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/"), nil
}

func normalizeRaftAddr(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || port == "" {
		return "", fmt.Errorf("node: raft address must be host:port")
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		return "", fmt.Errorf("node: raft address must be a specific IP")
	}
	return net.JoinHostPort(ip.String(), port), nil
}

// WaitForLeader waits until this server is the leader and its map includes every committed write.
// It also returns when ctx is canceled.
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
	payload, err := convert.EncodeCommand(cmd)
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
