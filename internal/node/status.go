package node

import (
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/hashicorp/raft"
)

const (
	ReadyMaxLag  uint64 = 64
	ReadyContact        = 3 * time.Second
)

type Peer struct {
	ID       string `json:"id"`
	RaftAddr string `json:"raft_addr"`
	HTTPAddr string `json:"http_addr,omitempty"`
	Suffrage string `json:"suffrage"`
}

// Status is a local sample of this process. Reading it does not append a log entry.
type Status struct {
	ID                string    `json:"id"`
	State             string    `json:"state"`
	Term              uint64    `json:"term"`
	LeaderID          string    `json:"leader_id,omitempty"`
	Leader            string    `json:"leader,omitempty"`
	CommitIndex       uint64    `json:"commit_index"`
	AppliedIndex      uint64    `json:"applied_index"`
	LastLogIndex      uint64    `json:"last_log_index"`
	LastSnapshotIndex uint64    `json:"last_snapshot_index"`
	LastContact       time.Time `json:"last_contact,omitempty"`
	Keys              int       `json:"keys"`
	Sessions          int       `json:"sessions"`
	Peers             []Peer    `json:"peers"`
}

func (n *Node) Status() (Status, error) {
	st := Status{
		ID:           n.id,
		State:        n.raft.State().String(),
		Term:         n.raft.CurrentTerm(),
		CommitIndex:  n.raft.CommitIndex(),
		AppliedIndex: n.applied(),
		LastLogIndex: n.raft.LastIndex(),
	}
	st.Keys, st.Sessions = n.fsm.sm.Counts()

	_, leaderID := n.raft.LeaderWithID()
	if leaderID != "" {
		st.LeaderID = string(leaderID)
		st.Leader = n.httpAddr(st.LeaderID)
	}
	if n.raft.State() != raft.Leader {
		if last := n.raft.LastContact(); !last.IsZero() {
			st.LastContact = last.UTC()
		}
	}

	snap, err := snapshotIndex(n.raft)
	if err != nil {
		return Status{}, err
	}
	st.LastSnapshotIndex = snap

	fut := n.raft.GetConfiguration()
	if err := fut.Error(); err != nil {
		return Status{}, fmt.Errorf("node: configuration: %w", err)
	}
	httpAddrs := n.httpAddrs()
	servers := fut.Configuration().Servers
	st.Peers = make([]Peer, 0, len(servers))
	for _, server := range servers {
		id := string(server.ID)
		st.Peers = append(st.Peers, Peer{
			ID:       id,
			RaftAddr: string(server.Address),
			HTTPAddr: httpAddrs[id],
			Suffrage: server.Suffrage.String(),
		})
	}
	return st, nil
}

func Ready(st Status, now time.Time) (bool, string) {
	switch st.State {
	case raft.Leader.String(), raft.Follower.String():
	default:
		if st.State == "" {
			return false, "unknown"
		}
		return false, strings.ToLower(st.State)
	}
	if st.LeaderID == "" {
		return false, "no leader"
	}
	if st.State == raft.Follower.String() {
		if st.LastContact.IsZero() {
			return false, "no leader contact"
		}
		if now.Sub(st.LastContact) > ReadyContact {
			return false, "leader contact is stale"
		}
	}
	if st.CommitIndex > st.AppliedIndex && st.CommitIndex-st.AppliedIndex > ReadyMaxLag {
		return false, fmt.Sprintf("apply lag %d", st.CommitIndex-st.AppliedIndex)
	}
	return true, ""
}

func (n *Node) httpAddrs() map[string]string {
	n.fsm.mu.RLock()
	defer n.fsm.mu.RUnlock()
	return maps.Clone(n.fsm.http)
}

func snapshotIndex(r *raft.Raft) (uint64, error) {
	raw, ok := r.Stats()["last_snapshot_index"]
	if !ok || raw == "" {
		return 0, nil
	}
	index, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("node: last snapshot index: %w", err)
	}
	return index, nil
}
