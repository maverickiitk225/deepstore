package node

import (
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

func TestReady(t *testing.T) {
	now := time.Date(2026, 11, 26, 12, 0, 0, 0, time.UTC)
	leader := Status{State: raft.Leader.String(), LeaderID: "n1", CommitIndex: 10, AppliedIndex: 10}
	follower := Status{
		State:        raft.Follower.String(),
		LeaderID:     "n1",
		CommitIndex:  10,
		AppliedIndex: 10,
		LastContact:  now.Add(-time.Second),
	}
	cases := []struct {
		name   string
		st     Status
		ready  bool
		reason string
	}{
		{name: "leader", st: leader, ready: true},
		{name: "follower", st: follower, ready: true},
		{name: "lag at the limit", st: Status{State: raft.Leader.String(), LeaderID: "n1", CommitIndex: ReadyMaxLag, AppliedIndex: 0}, ready: true},
		{name: "lag past the limit", st: Status{State: raft.Leader.String(), LeaderID: "n1", CommitIndex: ReadyMaxLag + 1, AppliedIndex: 0}, ready: false, reason: "apply lag 65"},
		{name: "no leader", st: Status{State: raft.Follower.String(), LastContact: now}, ready: false, reason: "no leader"},
		{name: "no contact", st: Status{State: raft.Follower.String(), LeaderID: "n1"}, ready: false, reason: "no leader contact"},
		{name: "stale contact", st: Status{State: raft.Follower.String(), LeaderID: "n1", LastContact: now.Add(-ReadyContact - time.Millisecond)}, ready: false, reason: "leader contact is stale"},
		{name: "candidate", st: Status{State: raft.Candidate.String(), LeaderID: "n1"}, ready: false, reason: "candidate"},
		{name: "shutdown", st: Status{State: raft.Shutdown.String()}, ready: false, reason: "shutdown"},
		{name: "empty state", st: Status{}, ready: false, reason: "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ok, reason := Ready(tc.st, now)
			if ok != tc.ready || reason != tc.reason {
				t.Fatalf("Ready = (%v, %q), want (%v, %q)", ok, reason, tc.ready, tc.reason)
			}
		})
	}
}

func TestStatusLeader(t *testing.T) {
	n := openNode(t, t.TempDir(), "127.0.0.1:0", 0)
	if _, err := n.Put("name", "ada", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := n.Advertise("http://127.0.0.1:7000"); err != nil {
		t.Fatal(err)
	}
	st, err := n.Status()
	if err != nil {
		t.Fatal(err)
	}
	if st.ID != "n1" || st.State != raft.Leader.String() || st.LeaderID != "n1" || st.Leader != "http://127.0.0.1:7000" {
		t.Fatalf("identity = %+v", st)
	}
	if st.Term == 0 || st.CommitIndex == 0 || st.AppliedIndex != st.CommitIndex || st.LastLogIndex < st.CommitIndex {
		t.Fatalf("indexes = term %d commit %d applied %d last %d", st.Term, st.CommitIndex, st.AppliedIndex, st.LastLogIndex)
	}
	if !st.LastContact.IsZero() {
		t.Fatalf("leader last contact = %s", st.LastContact)
	}
	if st.Keys != 1 || st.Sessions != 1 {
		t.Fatalf("counts = keys %d sessions %d", st.Keys, st.Sessions)
	}
	if len(st.Peers) != 1 || st.Peers[0].ID != "n1" || st.Peers[0].Suffrage != raft.Voter.String() || st.Peers[0].RaftAddr != n.RaftAddr() || st.Peers[0].HTTPAddr != "http://127.0.0.1:7000" {
		t.Fatalf("peers = %+v, raft addr %s", st.Peers, n.RaftAddr())
	}
	if ok, reason := Ready(st, time.Now()); !ok {
		t.Fatal(reason)
	}
}

func TestStatusFollower(t *testing.T) {
	n1 := openClusterNode(t, t.TempDir(), "n1", "127.0.0.1:0", true)
	n2 := openClusterNode(t, t.TempDir(), "n2", "127.0.0.1:0", false)
	if err := n1.AddServer("n2", n2.RaftAddr(), "http://127.0.0.1:7002", false); err != nil {
		t.Fatal(err)
	}
	if err := n1.Advertise("http://127.0.0.1:7000"); err != nil {
		t.Fatal(err)
	}
	st := waitStatus(t, n2, func(st Status) bool {
		return st.State == raft.Follower.String() &&
			st.LeaderID == "n1" &&
			st.Leader == "http://127.0.0.1:7000" &&
			!st.LastContact.IsZero() &&
			peerHTTP(st, "n1") == "http://127.0.0.1:7000" &&
			peerHTTP(st, "n2") == "http://127.0.0.1:7002"
	})
	if st.ID != "n2" {
		t.Fatalf("id = %s", st.ID)
	}
	if ok, reason := Ready(st, time.Now()); !ok {
		t.Fatal(reason)
	}
	leader, err := n1.Status()
	if err != nil {
		t.Fatal(err)
	}
	if leader.State != raft.Leader.String() {
		t.Fatalf("n1 state = %s", leader.State)
	}
}

func peerHTTP(st Status, id string) string {
	for _, peer := range st.Peers {
		if peer.ID == id {
			return peer.HTTPAddr
		}
	}
	return ""
}

func waitStatus(t *testing.T, n *Node, want func(Status) bool) Status {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var last Status
	var lastErr error
	for time.Now().Before(deadline) {
		st, err := n.Status()
		last, lastErr = st, err
		if err == nil && want(st) {
			return st
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("status = %+v, err %v", last, lastErr)
	return Status{}
}
