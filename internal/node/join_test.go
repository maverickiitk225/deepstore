package node

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
)

func TestAddServerRejectsDuplicateID(t *testing.T) {
	n1 := openClusterNode(t, t.TempDir(), "n1", "127.0.0.1:0", true)
	n2 := openClusterNode(t, t.TempDir(), "n2", "127.0.0.1:0", false)
	n3 := openClusterNode(t, t.TempDir(), "n3", "127.0.0.1:0", false)
	if err := n1.AddServer("n2", n2.RaftAddr(), "http://127.0.0.1:2", false); err != nil {
		t.Fatal(err)
	}
	if err := n1.AddServer("n3", n3.RaftAddr(), "http://127.0.0.1:3", false); err != nil {
		t.Fatal(err)
	}

	// Existing id, different address.
	if err := n1.AddServer("n2", n3.RaftAddr(), "http://127.0.0.1:9", false); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("duplicate id err = %v, want conflict", err)
	}
	if addr := mustServerAddr(t, n1, "n2"); addr != n2.RaftAddr() {
		t.Fatalf("n2 address after conflict = %s, want %s", addr, n2.RaftAddr())
	}
	if got := n1.httpAddr("n2"); got != "http://127.0.0.1:2" {
		t.Fatalf("n2 http after conflict = %q, want http://127.0.0.1:2", got)
	}

	// The leader's id can't be replaced.
	if err := n1.AddServer("n1", n2.RaftAddr(), "http://127.0.0.1:9", true); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("leader id err = %v, want conflict", err)
	}

	// Same id and address is a rejoin.
	if err := n1.AddServer("n2", n2.RaftAddr(), "http://127.0.0.1:22", false); err != nil {
		t.Fatalf("rejoin err = %v", err)
	}
	if got := n1.httpAddr("n2"); got != "http://127.0.0.1:22" {
		t.Fatalf("n2 http after rejoin = %q, want http://127.0.0.1:22", got)
	}

	// Move n3 to a new address.
	if err := n3.Close(); err != nil {
		t.Fatal(err)
	}
	moved := openClusterNode(t, t.TempDir(), "n3", "127.0.0.1:0", false)
	if err := n1.AddServer("n3", moved.RaftAddr(), "http://127.0.0.1:33", false); !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("move without replace err = %v, want conflict", err)
	}
	if err := n1.AddServer("n3", moved.RaftAddr(), "http://127.0.0.1:33", true); err != nil {
		t.Fatalf("move with replace err = %v", err)
	}
	if addr := mustServerAddr(t, n1, "n3"); addr != moved.RaftAddr() {
		t.Fatalf("n3 address after replace = %s, want %s", addr, moved.RaftAddr())
	}
	if n1.VoterCount() != 3 {
		t.Fatalf("voters = %d, want 3", n1.VoterCount())
	}
	if _, err := n1.Put("k", "v", 1, 1); err != nil {
		t.Fatal(err)
	}
	waitApplied(t, moved, n1.applied())
}

func TestJoinStopsOnConflict(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"node: conflict: id n2 is already a server at 127.0.0.1:7003"}`))
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := Join(ctx, srv.URL, "n2", "127.0.0.1:7999", "http://127.0.0.1:7998", false)
	if !errors.Is(err, errs.ErrConflict) {
		t.Fatalf("Join err = %v, want conflict", err)
	}
	if ctx.Err() != nil {
		t.Fatal("Join waited for the context instead of stopping")
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("join requests = %d, want 1", n)
	}
}

func mustServerAddr(t *testing.T, n *Node, id string) string {
	t.Helper()
	addr, ok, err := n.serverAddr(id)
	if err != nil || !ok {
		t.Fatalf("serverAddr(%s) = (%q, %v, %v)", id, addr, ok, err)
	}
	return addr
}
