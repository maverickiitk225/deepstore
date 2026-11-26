package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/node"
	"github.com/prometheus/client_golang/prometheus"
)

func TestHealthWithoutStatus(t *testing.T) {
	ts := serveHTTP(t, keyStore{})
	if got := getStatus(t, ts.URL+"/healthz"); got != http.StatusOK {
		t.Fatalf("healthz = %d, want 200", got)
	}
	if got := getStatus(t, ts.URL+"/readyz"); got != http.StatusNotFound {
		t.Fatalf("readyz = %d, want 404", got)
	}
	if got := getStatus(t, ts.URL+"/v1/status"); got != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", got)
	}
}

func TestStatusAndReady(t *testing.T) {
	now := time.Now()
	leader := &memStatus{st: node.Status{
		ID:           "n1",
		State:        "Leader",
		LeaderID:     "n1",
		Leader:       "http://127.0.0.1:7000",
		CommitIndex:  4,
		AppliedIndex: 4,
	}}
	ts := serveHTTP(t, leader)

	resp, err := http.Get(ts.URL + "/v1/keys/k")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("GET key = %d, want 503", resp.StatusCode)
	}

	body := getJSON[node.Status](t, ts.URL+"/v1/status")
	if body.ID != "n1" || body.State != "Leader" || body.Leader != "http://127.0.0.1:7000" {
		t.Fatalf("status = %+v", body)
	}
	health := getJSON[healthBody](t, ts.URL+"/readyz")
	if !health.OK {
		t.Fatalf("readyz = %+v", health)
	}

	leader.st.State = "Follower"
	leader.st.LeaderID = ""
	notReady := getJSON[healthBody](t, ts.URL+"/readyz")
	if notReady.OK || notReady.Reason != "no leader" {
		t.Fatalf("readyz = %+v, want no leader", notReady)
	}
	if got := getStatus(t, ts.URL+"/v1/status"); got != http.StatusOK {
		t.Fatalf("follower status = %d, want 200", got)
	}

	leader.st.LeaderID = "n1"
	leader.st.LastContact = now
	leader.now = true
	if got := getStatus(t, ts.URL+"/readyz"); got != http.StatusOK {
		t.Fatalf("fresh follower = %d, want 200", got)
	}

	leader.now = false
	leader.st.LastContact = time.Time{}
	stale := getJSON[healthBody](t, ts.URL+"/readyz")
	if stale.Reason != "no leader contact" {
		t.Fatalf("readyz = %+v, want no leader contact", stale)
	}

	leader.st.State = "Leader"
	leader.st.CommitIndex = node.ReadyMaxLag + 1
	leader.st.AppliedIndex = 0
	lag := getJSON[healthBody](t, ts.URL+"/readyz")
	if lag.Reason != "apply lag 65" {
		t.Fatalf("readyz = %+v, want apply lag", lag)
	}

	leader.err = errors.New("raft is shut down")
	if got := getStatus(t, ts.URL+"/v1/status"); got != http.StatusInternalServerError {
		t.Fatalf("status error = %d, want 500", got)
	}
	down := getJSON[healthBody](t, ts.URL+"/readyz")
	if down.OK || down.Reason != "raft is shut down" {
		t.Fatalf("readyz = %+v", down)
	}
}

func TestMetricsIncludeRequest(t *testing.T) {
	ts := serveHTTP(t, keyStore{})
	if got := getStatus(t, ts.URL+"/healthz"); got != http.StatusOK {
		t.Fatalf("healthz = %d", got)
	}
	resp, err := http.Get(ts.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("metrics = %d, want 200", resp.StatusCode)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `route="/healthz"`) {
		t.Fatal("metrics scrape has no /healthz series")
	}
}

func TestNodeCollectorGauges(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newNodeCollector(statusFunc(func() (node.Status, error) {
		return node.Status{
			State:        "Leader",
			Term:         3,
			CommitIndex:  10,
			AppliedIndex: 8,
			Keys:         2,
			Sessions:     1,
		}, nil
	})))
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]float64{}
	for _, mf := range families {
		if len(mf.GetMetric()) != 1 || mf.GetMetric()[0].GetGauge() == nil {
			t.Fatalf("%s = %v", mf.GetName(), mf.GetMetric())
		}
		got[mf.GetName()] = mf.GetMetric()[0].GetGauge().GetValue()
	}
	want := map[string]float64{
		"deepstore_raft_commit_index":  10,
		"deepstore_raft_applied_index": 8,
		"deepstore_raft_apply_lag":     2,
		"deepstore_raft_term":          3,
		"deepstore_raft_leader":        1,
		"deepstore_keys":               2,
		"deepstore_sessions":           1,
	}
	for name, value := range want {
		if got[name] != value {
			t.Fatalf("%s = %v, want %v", name, got[name], value)
		}
	}
}

type keyStore struct{}

func (keyStore) Put(string, string, uint64, uint64) (uint64, error) { return 0, nil }
func (keyStore) Delete(string, uint64, uint64) (uint64, error)      { return 0, nil }
func (keyStore) CAS(string, string, string, uint64, uint64) (uint64, bool, error) {
	return 0, false, nil
}
func (keyStore) Get(string) (string, bool, error) { return "", false, nil }

type memStatus struct {
	st  node.Status
	err error
	now bool
}

func (m *memStatus) Put(string, string, uint64, uint64) (uint64, error) { return 0, nil }
func (m *memStatus) Delete(string, uint64, uint64) (uint64, error)      { return 0, nil }
func (m *memStatus) CAS(string, string, string, uint64, uint64) (uint64, bool, error) {
	return 0, false, nil
}
func (m *memStatus) Get(string) (string, bool, error) {
	return "", false, fmt.Errorf("node: %w", errs.ErrNotLeader)
}
func (m *memStatus) LeaderHTTP() string { return m.st.Leader }
func (m *memStatus) Status() (node.Status, error) {
	if m.err != nil {
		return node.Status{}, m.err
	}
	st := m.st
	if m.now {
		st.LastContact = time.Now()
	}
	return st, nil
}

type statusFunc func() (node.Status, error)

func (f statusFunc) Status() (node.Status, error) { return f() }

func serveHTTP(t *testing.T, store storage) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(New(store, "127.0.0.1:0").Handler())
	t.Cleanup(ts.Close)
	return ts
}

func getStatus(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func getJSON[T any](t *testing.T, url string) T {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body T
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	return body
}
