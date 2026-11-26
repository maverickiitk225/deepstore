package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/client"
	"github.com/deepanker/deepstore/internal/engine"
	"github.com/deepanker/deepstore/internal/errs"
)

type engineStore struct{ *engine.Engine }

func (e engineStore) Get(key string) (string, bool, error) {
	value, ok := e.Engine.Get(key)
	return value, ok, nil
}

func TestCASRoundTrip(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(engineStore{e}, ln.Addr().String())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Serve(ln)
	}()

	c := client.New("http://" + ln.Addr().String())
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", "old"); err != nil {
		t.Fatal(err)
	}
	index, swapped, err := c.CAS(ctx, "k", "stale", "nope")
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 || swapped {
		t.Fatalf("CAS mismatch = (%d, %v), want (2, false)", index, swapped)
	}
	index, swapped, err = c.CAS(ctx, "k", "old", "new")
	if err != nil {
		t.Fatal(err)
	}
	if index != 3 || !swapped {
		t.Fatalf("CAS match = (%d, %v), want (3, true)", index, swapped)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || got != "new" {
		t.Fatalf("Get(k) = (%q, %v, %v), want (new, true, nil)", got, ok, err)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}

	e, err = engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if v, ok := e.Get("k"); !ok || v != "new" {
		t.Fatalf("Get(k) after restart = (%q, %v), want (new, true)", v, ok)
	}
}

func TestSessionRetryAndGap(t *testing.T) {
	e, err := engine.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(engineStore{e}, ln.Addr().String())
	go func() {
		_ = s.Serve(ln)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
		_ = e.Close()
	})

	c := client.New("http://" + ln.Addr().String())
	ctx := context.Background()
	seq := c.NextSeq()
	index, err := c.PutSeq(ctx, "k", "v", seq)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.PutSeq(ctx, "k", "other", seq)
	if err != nil {
		t.Fatal(err)
	}
	if again != index {
		t.Fatalf("retry index = %d, want %d", again, index)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || got != "v" {
		t.Fatalf("Get = (%q, %v, %v), want (v, true, nil)", got, ok, err)
	}
	if _, err := c.PutSeq(ctx, "k", "nope", seq+2); err == nil {
		t.Fatal("gap: err = nil, want error")
	}
}

func TestPutGetDeleteRoundTrip(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(engineStore{e}, ln.Addr().String())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Serve(ln)
	}()

	c := client.New("http://" + ln.Addr().String())
	ctx := context.Background()
	index, err := c.Put(ctx, "k", "v")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("Put index = %d, want 1", index)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if !ok || got != "v" {
		t.Fatalf("Get(k) = (%q, %v), want (v, true)", got, ok)
	}
	index, err = c.Delete(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 {
		t.Fatalf("Delete index = %d, want 2", index)
	}
	if _, ok, err := c.Get(ctx, "k"); err != nil || ok {
		t.Fatalf("Get after delete = ok %v, err %v; want ok false", ok, err)
	}
	if _, ok, err := c.Get(ctx, "missing"); err != nil || ok {
		t.Fatalf("Get missing = ok %v, err %v; want ok false", ok, err)
	}
	if _, err := c.Put(ctx, "keep", "yes"); err != nil {
		t.Fatal(err)
	}

	req, err := http.NewRequest(http.MethodPut, "http://"+ln.Addr().String()+"/v1/keys/bad", strings.NewReader("{"))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json status = %d, want 400", resp.StatusCode)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}

	if _, _, err := c.Get(ctx, "k"); err == nil {
		t.Fatal("Get after shutdown: err = nil, want error")
	}

	e, err = engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, ok := e.Get("k"); ok {
		t.Fatal("Get(k) after restart: ok = true, want false")
	}
	if v, ok := e.Get("keep"); !ok || v != "yes" {
		t.Fatalf("Get(keep) after restart = (%q, %v), want (yes, true)", v, ok)
	}
}

func TestBinaryValueRoundTrip(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(engineStore{e}, ln.Addr().String())
	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Serve(ln)
	}()

	value := string([]byte{0xff, 0xfe, 0x00, 'a'})
	next := string([]byte{0x80, 0x81})
	c := client.New("http://" + ln.Addr().String())
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", value); err != nil {
		t.Fatal(err)
	}
	if stored, ok := e.Get("k"); !ok || stored != value {
		t.Fatalf("stored = %q ok %v, want the original bytes", stored, ok)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || got != value {
		t.Fatalf("Get = (%q, %v, %v), want the original bytes", got, ok, err)
	}
	index, swapped, err := c.CAS(ctx, "k", value, next)
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 || !swapped {
		t.Fatalf("CAS = (%d, %v), want (2, true)", index, swapped)
	}
	if stored, ok := e.Get("k"); !ok || stored != next {
		t.Fatalf("stored after CAS = %q ok %v, want the new bytes", stored, ok)
	}

	c2 := client.New("http://" + ln.Addr().String())
	if _, err := c2.PutSeq(ctx, "other", value, 1); err != nil {
		t.Fatal(err)
	}
	if stored, ok := e.Get("other"); !ok || stored != value {
		t.Fatalf("PutSeq stored = %q ok %v, want the original bytes", stored, ok)
	}
	if _, swapped, err := c2.CASSeq(ctx, "other", value, next, 2); err != nil || !swapped {
		t.Fatalf("CASSeq swapped = %v, err %v; want true, nil", swapped, err)
	}

	req, err := http.NewRequest(http.MethodPut, "http://"+ln.Addr().String()+"/v1/keys/bad", strings.NewReader(`{"value":"!!!!","client_id":1,"seq":1}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid base64 status = %d, want 400", resp.StatusCode)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}

type joinStore struct {
	leader   string
	id       string
	raftAddr string
	httpAddr string
	replace  bool
}

func (j *joinStore) Put(string, string, uint64, uint64) (uint64, error) {
	return 0, fmt.Errorf("node: %w", errs.ErrNotLeader)
}

func (j *joinStore) Delete(string, uint64, uint64) (uint64, error) {
	return 0, fmt.Errorf("node: %w", errs.ErrNotLeader)
}

func (j *joinStore) CAS(string, string, string, uint64, uint64) (uint64, bool, error) {
	return 0, false, fmt.Errorf("node: %w", errs.ErrNotLeader)
}

func (j *joinStore) Get(string) (string, bool, error) {
	return "", false, fmt.Errorf("node: %w", errs.ErrNotLeader)
}

func (j *joinStore) LeaderHTTP() string { return j.leader }

func (j *joinStore) AddServer(id, raftAddr, httpAddr string, replace bool) error {
	if j.leader != "" {
		return fmt.Errorf("node: %w", errs.ErrNotLeader)
	}
	if id == "taken" && !replace {
		return fmt.Errorf("node: %w: id taken is already a server at 127.0.0.1:9", errs.ErrConflict)
	}
	j.id, j.raftAddr, j.httpAddr, j.replace = id, raftAddr, httpAddr, replace
	return nil
}

func TestClientFollowsLeader(t *testing.T) {
	dir := t.TempDir()
	e, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	leaderLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	leader := New(engineStore{e}, leaderLn.Addr().String())
	leaderErr := make(chan error, 1)
	go func() { leaderErr <- leader.Serve(leaderLn) }()

	followerLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	follower := New(&joinStore{leader: "http://" + leaderLn.Addr().String()}, followerLn.Addr().String())
	followerErr := make(chan error, 1)
	go func() { followerErr <- follower.Serve(followerLn) }()

	c := client.New("http://" + followerLn.Addr().String())
	ctx := context.Background()
	index, err := c.Put(ctx, "k", "v")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("Put index = %d, want 1", index)
	}
	got, ok, err := c.Get(ctx, "k")
	if err != nil || !ok || got != "v" {
		t.Fatalf("Get = (%q, %v, %v), want (v, true, nil)", got, ok, err)
	}

	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := follower.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	if err := leader.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	for _, ch := range []chan error{leaderErr, followerErr} {
		select {
		case err := <-ch:
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				t.Fatal(err)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not stop")
		}
	}
}

func TestJoinRoute(t *testing.T) {
	st := &joinStore{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(st, ln.Addr().String())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ln) }()
	base := "http://" + ln.Addr().String()

	resp, err := http.Post(base+"/v1/join", "application/json", strings.NewReader(`{"id":"n2","raft_addr":"127.0.0.1:7003","http_addr":"http://127.0.0.1:7002"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("join status = %d, want 204", resp.StatusCode)
	}
	if st.id != "n2" || st.raftAddr != "127.0.0.1:7003" || st.httpAddr != "http://127.0.0.1:7002" {
		t.Fatalf("joined %+v", st)
	}

	resp, err = http.Post(base+"/v1/join", "application/json", strings.NewReader(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("empty join status = %d, want 400", resp.StatusCode)
	}

	resp, err = http.Post(base+"/v1/join", "application/json", strings.NewReader(`{"id":"taken","raft_addr":"127.0.0.1:7003","http_addr":"http://127.0.0.1:7002"}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("duplicate id join status = %d, want 409", resp.StatusCode)
	}

	resp, err = http.Post(base+"/v1/join", "application/json", strings.NewReader(`{"id":"taken","raft_addr":"127.0.0.1:7003","http_addr":"http://127.0.0.1:7002","replace":true}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("replace join status = %d, want 204", resp.StatusCode)
	}
	if st.id != "taken" || !st.replace {
		t.Fatalf("replace joined %+v", st)
	}

	st.leader = "http://127.0.0.1:7000"
	resp, err = http.Post(base+"/v1/join", "application/json", strings.NewReader(`{"id":"n3","raft_addr":"127.0.0.1:7005","http_addr":"http://127.0.0.1:7004"}`))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("follower join status = %d, want 503", resp.StatusCode)
	}
	var body struct {
		Leader string `json:"leader"`
	}
	if err := json.Unmarshal(data, &body); err != nil {
		t.Fatal(err)
	}
	if body.Leader != "http://127.0.0.1:7000" {
		t.Fatalf("leader = %q", body.Leader)
	}

	// Close unused client connections, or Shutdown waits 5 seconds for them.
	http.DefaultClient.CloseIdleConnections()
	shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Shutdown(shutCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("server did not stop")
	}
}
