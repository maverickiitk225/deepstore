package main

import (
	"io"
	"net"
	"os"
	"testing"

	"github.com/deepanker/deepstore/internal/node"
)

func TestResolveBootstrap(t *testing.T) {
	refused := "refusing -bootstrap because the join peer is not reachable. Omit -bootstrap to wait for it, or omit -join to create a new cluster"
	empty := "data directory has no cluster state: pass -bootstrap to create a cluster, or -join to join one"
	cases := []struct {
		name          string
		hasState      bool
		wantBootstrap bool
		join          string
		peerReachable bool
		want          bool
		wantErr       string
	}{
		{name: "empty dir bootstraps only with the flag", wantBootstrap: true, want: true},
		{name: "empty dir without a flag exits", wantErr: empty},
		{name: "join does not bootstrap", join: "http://127.0.0.1:7000"},
		{name: "existing state ignores bootstrap", hasState: true, wantBootstrap: true},
		{name: "reachable peer is joined", wantBootstrap: true, join: "http://127.0.0.1:7000", peerReachable: true},
		{name: "unreachable peer refuses bootstrap", wantBootstrap: true, join: "http://127.0.0.1:7000", wantErr: refused},
		{name: "restart with both flags does not bootstrap", hasState: true, wantBootstrap: true, join: "http://127.0.0.1:7000"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveBootstrap(tc.hasState, tc.wantBootstrap, tc.join, tc.peerReachable)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("err = %v, want %s", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tc.want {
				t.Fatalf("bootstrap = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestJoinPeerReachable(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	ok, err := joinPeerReachable("http://" + ln.Addr().String())
	if err != nil || !ok {
		t.Fatalf("open listener = (%v, %v), want (true, nil)", ok, err)
	}
	ok, err = joinPeerReachable(ln.Addr().String())
	if err != nil || !ok {
		t.Fatalf("address without scheme = (%v, %v), want (true, nil)", ok, err)
	}

	closed, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := closed.Addr().String()
	if err := closed.Close(); err != nil {
		t.Fatal(err)
	}
	ok, err = joinPeerReachable("http://" + addr)
	if err != nil || ok {
		t.Fatalf("closed port = (%v, %v), want (false, nil)", ok, err)
	}
	if _, err := joinPeerReachable("http://127.0.0.1"); err == nil {
		t.Fatal("address without a port err = nil")
	}
}

func TestServeEmptyDirRequiresBootstrap(t *testing.T) {
	dir := t.TempDir()
	code, out := serveQuiet(t, "-data-dir", dir)
	if code != 2 {
		t.Fatalf("exit = %d, want 2\n%s", code, out)
	}
	if has, err := node.HasExistingState(dir); err != nil || has {
		t.Fatalf("state = (%v, %v), want (false, nil)", has, err)
	}
}

func TestServeRefusesBootstrapWhenJoinIsDown(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	code, out := serveQuiet(t, "-data-dir", dir, "-bootstrap", "-join", "http://"+addr)
	if code != 2 {
		t.Fatalf("exit = %d, want 2\n%s", code, out)
	}
	if has, err := node.HasExistingState(dir); err != nil || has {
		t.Fatalf("state = (%v, %v), want (false, nil)", has, err)
	}
}

func serveQuiet(t *testing.T, args ...string) (int, string) {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stderr
	os.Stderr = w
	defer func() { os.Stderr = old }()
	code := serve(args)
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	out, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	return code, string(out)
}
