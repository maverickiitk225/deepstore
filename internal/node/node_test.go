package node

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/snapshot"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

func TestPutGetReopen(t *testing.T) {
	dir := t.TempDir()
	n := openNode(t, dir, "127.0.0.1:0", 0)
	addr := n.RaftAddr()
	index, err := n.Put("name", "ada", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if index == 0 {
		t.Fatal("index = 0")
	}
	got, ok, err := n.Get("name")
	if err != nil || !ok || got != "ada" {
		t.Fatalf("Get = (%q, %v, %v), want (ada, true, nil)", got, ok, err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	n = openNode(t, dir, addr, 0)
	defer n.Close()
	got, ok, err = n.Get("name")
	if err != nil || !ok || got != "ada" {
		t.Fatalf("Get after reopen = (%q, %v, %v), want (ada, true, nil)", got, ok, err)
	}
	again, err := n.Put("name", "other", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	if again != index {
		t.Fatalf("retry index = %d, want %d", again, index)
	}
	got, ok, err = n.Get("name")
	if err != nil || !ok || got != "ada" {
		t.Fatalf("Get after retry = (%q, %v, %v), want (ada, true, nil)", got, ok, err)
	}
}

func TestSessionGapAndCAS(t *testing.T) {
	n := openNode(t, t.TempDir(), "127.0.0.1:0", 0)
	defer n.Close()

	index, err := n.Put("k", "old", 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := n.Put("k", "nope", 4, 3); !errors.Is(err, errs.ErrSession) {
		t.Fatalf("gap err = %v, want session", err)
	}
	again, err := n.Put("k", "other", 4, 1)
	if err != nil || again != index {
		t.Fatalf("retry = (%d, %v), want (%d, nil)", again, err, index)
	}

	casIndex, swapped, err := n.CAS("k", "stale", "nope", 4, 2)
	if err != nil || swapped || casIndex != index+1 {
		t.Fatalf("CAS miss = (%d, %v, %v), want (%d, false, nil)", casIndex, swapped, err, index+1)
	}
	casIndex, swapped, err = n.CAS("k", "old", "new", 4, 3)
	if err != nil || !swapped || casIndex != index+2 {
		t.Fatalf("CAS hit = (%d, %v, %v), want (%d, true, nil)", casIndex, swapped, err, index+2)
	}
	retry, swapped, err := n.CAS("k", "old", "other", 4, 3)
	if err != nil || !swapped || retry != casIndex {
		t.Fatalf("CAS retry = (%d, %v, %v), want (%d, true, nil)", retry, swapped, err, casIndex)
	}

	del, err := n.Delete("k", 4, 4)
	if err != nil || del != casIndex+1 {
		t.Fatalf("Delete = (%d, %v), want (%d, nil)", del, err, casIndex+1)
	}
	if _, ok, err := n.Get("k"); err != nil || ok {
		t.Fatalf("Get after delete = ok %v, err %v; want ok false", ok, err)
	}
}

func TestSnapshotReopen(t *testing.T) {
	dir := t.TempDir()
	n := openNode(t, dir, "127.0.0.1:0", 2)
	addr := n.RaftAddr()
	for seq := uint64(1); seq <= 4; seq++ {
		if _, err := n.Put("k", string(rune('a'+seq-1)), 9, seq); err != nil {
			t.Fatal(err)
		}
	}
	waitSnapshot(t, dir)
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	n = openNode(t, dir, addr, 2)
	defer n.Close()
	got, ok, err := n.Get("k")
	if err != nil || !ok || got != "d" {
		t.Fatalf("Get after snapshot = (%q, %v, %v), want (d, true, nil)", got, ok, err)
	}
	if _, err := n.Put("k", "e", 9, 4); err != nil {
		t.Fatal(err)
	}
	got, ok, err = n.Get("k")
	if err != nil || !ok || got != "d" {
		t.Fatalf("Get after session retry = (%q, %v, %v), want (d, true, nil)", got, ok, err)
	}
}

func openNode(t *testing.T, dir, addr string, threshold uint64) *Node {
	t.Helper()
	rc := raft.DefaultConfig()
	rc.HeartbeatTimeout = 100 * time.Millisecond
	rc.ElectionTimeout = 100 * time.Millisecond
	rc.LeaderLeaseTimeout = 50 * time.Millisecond
	rc.CommitTimeout = 5 * time.Millisecond
	rc.SnapshotInterval = 50 * time.Millisecond
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Off, Output: io.Discard})
	n, err := Open(Config{
		Dir:               dir,
		NodeID:            "n1",
		RaftAddr:          addr,
		Bootstrap:         true,
		SnapshotThreshold: threshold,
		LogOutput:         io.Discard,
		Raft:              rc,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.WaitForLeader(ctx); err != nil {
		_ = n.Close()
		t.Fatal(err)
	}
	return n
}

func TestJoinFailover(t *testing.T) {
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	dir3 := t.TempDir()
	n1 := openClusterNode(t, dir1, "n1", "127.0.0.1:0", true)
	n2 := openClusterNode(t, dir2, "n2", "127.0.0.1:0", false)
	n3 := openClusterNode(t, dir3, "n3", "127.0.0.1:0", false)
	if n2.raft.State() == raft.Leader || n3.raft.State() == raft.Leader {
		t.Fatal("node became leader before joining")
	}

	if err := n1.AddServer("n2", n2.RaftAddr(), "http://127.0.0.1:2"); err != nil {
		t.Fatal(err)
	}
	if err := n1.AddServer("n3", n3.RaftAddr(), "http://127.0.0.1:3"); err != nil {
		t.Fatal(err)
	}
	if n1.VoterCount() != 3 {
		t.Fatalf("voters = %d, want 3", n1.VoterCount())
	}
	if _, err := n2.Put("k", "v", 1, 1); !errors.Is(err, errs.ErrNotLeader) {
		t.Fatalf("follower put err = %v, want not leader", err)
	}
	if err := n1.Advertise("http://127.0.0.1:1"); err != nil {
		t.Fatal(err)
	}
	index, err := n1.Put("k", "v", 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	waitApplied(t, n2, n1.applied())
	waitApplied(t, n3, n1.applied())
	if got := n2.LeaderHTTP(); got != "http://127.0.0.1:1" {
		t.Fatalf("leader http = %q, want http://127.0.0.1:1", got)
	}
	again, err := n2.Put("k", "other", 1, 1)
	if err != nil || again != index {
		t.Fatalf("follower retry = (%d, %v), want (%d, nil)", again, err, index)
	}
	if _, _, err := n2.Get("k"); !errors.Is(err, errs.ErrNotLeader) {
		t.Fatalf("follower get err = %v, want not leader", err)
	}

	addr2, addr3 := n2.RaftAddr(), n3.RaftAddr()
	if err := n1.Close(); err != nil {
		t.Fatal(err)
	}
	leader := waitAnyLeader(t, n2, n3)
	got, ok, err := leader.Get("k")
	if err != nil || !ok || got != "v" {
		t.Fatalf("Get after failover = (%q, %v, %v), want (v, true, nil)", got, ok, err)
	}
	if _, err := leader.Put("k", "w", 2, 1); err != nil {
		t.Fatal(err)
	}

	if err := n2.Close(); err != nil {
		t.Fatal(err)
	}
	if err := n3.Close(); err != nil {
		t.Fatal(err)
	}
	n2 = openClusterNode(t, dir2, "n2", addr2, false)
	n3 = openClusterNode(t, dir3, "n3", addr3, false)
	leader = waitAnyLeader(t, n2, n3)
	if leader.VoterCount() != 3 {
		t.Fatalf("voters after reopen = %d, want 3", leader.VoterCount())
	}
	got, ok, err = leader.Get("k")
	if err != nil || !ok || got != "w" {
		t.Fatalf("Get after reopen = (%q, %v, %v), want (w, true, nil)", got, ok, err)
	}
}

func TestSnapshotKeepsPeerHTTP(t *testing.T) {
	f := newFSM()
	if _, err := f.sm.Apply(store.Command{
		Type: store.CommandTypePut, Key: "k", Value: "v", ClientID: 1, Seq: 1, Index: 1,
	}); err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.index = 4
	f.http["n1"] = "http://127.0.0.1:1"
	f.mu.Unlock()

	snap, err := f.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var sink memSink
	if err := snap.Persist(&sink); err != nil {
		t.Fatal(err)
	}
	restored := newFSM()
	if err := restored.Restore(io.NopCloser(bytes.NewReader(sink.Bytes()))); err != nil {
		t.Fatal(err)
	}
	if restored.http["n1"] != "http://127.0.0.1:1" || restored.index != 4 {
		t.Fatalf("restored peers = %v index %d", restored.http, restored.index)
	}
	if got, ok := restored.sm.Get("k"); !ok || got != "v" {
		t.Fatalf("restored Get = (%q, %v)", got, ok)
	}

	frame, err := snapshot.Encode(snapshot.Snapshot{Index: 1, Data: map[string]string{"k": "v"}, Sessions: map[uint64]snapshot.Session{
		1: {Seq: 1, Index: 1},
	}})
	if err != nil {
		t.Fatal(err)
	}
	bare := newFSM()
	bare.http["stale"] = "http://old"
	if err := bare.Restore(io.NopCloser(bytes.NewReader(frame))); err != nil {
		t.Fatal(err)
	}
	if len(bare.http) != 0 {
		t.Fatalf("peers = %v, want empty", bare.http)
	}
}

type memSink struct {
	bytes.Buffer
}

func (m *memSink) ID() string { return "mem" }

func (m *memSink) Cancel() error { return nil }

func (m *memSink) Close() error { return nil }

func openClusterNode(t *testing.T, dir, id, addr string, bootstrap bool) *Node {
	t.Helper()
	n, err := Open(Config{
		Dir:               dir,
		NodeID:            id,
		RaftAddr:          addr,
		Bootstrap:         bootstrap,
		SnapshotThreshold: 0,
		LogOutput:         io.Discard,
		Raft:              testRaftConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = n.Close() })
	if !bootstrap {
		return n
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := n.WaitForLeader(ctx); err != nil {
		_ = n.Close()
		t.Fatal(err)
	}
	return n
}

func testRaftConfig() *raft.Config {
	rc := raft.DefaultConfig()
	rc.HeartbeatTimeout = 50 * time.Millisecond
	rc.ElectionTimeout = 150 * time.Millisecond
	rc.LeaderLeaseTimeout = 40 * time.Millisecond
	rc.CommitTimeout = 5 * time.Millisecond
	rc.SnapshotInterval = time.Hour
	rc.Logger = hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Off, Output: io.Discard})
	return rc
}

func waitApplied(t *testing.T, n *Node, index uint64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n.applied() >= index {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("applied %d, want >= %d", n.applied(), index)
}

func waitAnyLeader(t *testing.T, nodes ...*Node) *Node {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.raft.State() == raft.Leader && n.raft.Barrier(applyTimeout).Error() == nil {
				return n
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader")
	return nil
}

func waitSnapshot(t *testing.T, dir string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		entries, err := os.ReadDir(filepath.Join(dir, "raft-snap"))
		if err == nil && len(entries) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("snapshot was not written")
}
