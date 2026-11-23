package node

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
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
