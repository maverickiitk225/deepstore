package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestUncertainClassifiesOutcome(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "session", err: &statusError{code: http.StatusBadRequest, msg: "node: session: client 1 seq 2, want 1"}, want: false},
		{name: "too large", err: &statusError{code: http.StatusInternalServerError, msg: "convert: command too large"}, want: false},
		{name: "empty key", err: &statusError{code: http.StatusInternalServerError, msg: "node: empty key"}, want: false},
		{name: "enqueue", err: &statusError{code: http.StatusInternalServerError, msg: "timed out enqueuing operation"}, want: true},
		{name: "shutdown", err: &statusError{code: http.StatusInternalServerError, msg: "raft is already shutdown"}, want: true},
		{name: "not leader", err: &statusError{code: http.StatusServiceUnavailable, msg: "node: not leader"}, want: true},
		{name: "not found", err: errNotFound, want: false},
		{name: "refused", err: &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}, want: false},
		{name: "timeout", err: context.DeadlineExceeded, want: true},
		{name: "eof", err: io.EOF, want: true},
		{name: "decode", err: errors.New("client: decode response: unexpected EOF"), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := uncertain(tc.err); got != tc.want {
				t.Fatalf("uncertain = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestPutReusesSequenceAfterRejection(t *testing.T) {
	var mu sync.Mutex
	var seqs []uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, value := readSeq(t, r)
		mu.Lock()
		seqs = append(seqs, seq)
		mu.Unlock()
		if value == "big" {
			writeStatus(w, http.StatusInternalServerError, "convert: command too large")
			return
		}
		writeResult(w, seq, false)
	}))
	defer srv.Close()

	c := New(srv.URL)
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", "big"); err == nil {
		t.Fatal("oversized put err = nil, want error")
	}
	index, err := c.Put(ctx, "k", "ok")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("index = %d, want 1", index)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 1 {
		t.Fatalf("seqs = %v, want [1 1]", seqs)
	}
}

func TestPutRetriesUnknownWithSameSequence(t *testing.T) {
	var mu sync.Mutex
	var seqs []uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, _ := readSeq(t, r)
		mu.Lock()
		seqs = append(seqs, seq)
		n := len(seqs)
		mu.Unlock()
		if n == 1 {
			writeStatus(w, http.StatusInternalServerError, "timed out enqueuing operation")
			return
		}
		writeResult(w, seq, false)
	}))
	defer srv.Close()

	index, err := New(srv.URL).Put(context.Background(), "k", "v")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("index = %d, want 1", index)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 2 || seqs[0] != 1 || seqs[1] != 1 {
		t.Fatalf("seqs = %v, want [1 1]", seqs)
	}
}

func TestPutRetriesNotLeader(t *testing.T) {
	var n int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, _ := readSeq(t, r)
		n++
		if n == 1 {
			writeStatus(w, http.StatusServiceUnavailable, "node: not leader")
			return
		}
		writeResult(w, seq, false)
	}))
	defer srv.Close()

	index, err := New(srv.URL).Put(context.Background(), "k", "v")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 || n != 2 {
		t.Fatalf("index = %d, attempts = %d, want 1 and 2", index, n)
	}
}

func TestDialErrorDoesNotConsumeSequence(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	c := New("http://" + addr)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	_, err = c.Put(ctx, "k", "v")
	cancel()
	if err == nil {
		t.Fatal("put to closed port err = nil, want error")
	}

	var seqs []uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, _ := readSeq(t, r)
		seqs = append(seqs, seq)
		writeResult(w, seq, false)
	}))
	defer srv.Close()
	c.setBase(srv.URL)

	index, err := c.Put(context.Background(), "k", "v")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 || len(seqs) != 1 || seqs[0] != 1 {
		t.Fatalf("index = %d, seqs = %v, want 1 and [1]", index, seqs)
	}
}

func TestLostResponseRetriesSameCommand(t *testing.T) {
	c, calls := lostResponseServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.Put(ctx, "k", "v")
		errc <- err
	}()
	waitForCall(t, calls, 1)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("first put err = %v, want canceled", err)
	}

	index, err := c.Put(context.Background(), "k", "v")
	if err != nil {
		t.Fatal(err)
	}
	if index != 7 {
		t.Fatalf("index = %d, want 7", index)
	}
	got := snapshot(calls)
	if len(got) != 2 || got[0].seq != 1 || got[1].seq != 1 {
		t.Fatalf("calls = %+v, want seq 1 twice", got)
	}
}

func TestLostResponseThenDifferentCommand(t *testing.T) {
	c, calls := lostResponseServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() {
		_, err := c.Put(ctx, "k", "v")
		errc <- err
	}()
	waitForCall(t, calls, 1)
	cancel()
	if err := <-errc; !errors.Is(err, context.Canceled) {
		t.Fatalf("first put err = %v, want canceled", err)
	}

	index, err := c.Put(context.Background(), "other", "w")
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 {
		t.Fatalf("index = %d, want 2", index)
	}
	got := snapshot(calls)
	if len(got) != 3 || got[0].seq != 1 || got[1].seq != 1 || got[2].seq != 2 {
		t.Fatalf("calls = %+v, want seq 1, 1, 2", got)
	}
	if got[2].key != "other" {
		t.Fatalf("last key = %q, want other", got[2].key)
	}
}

func TestWritesShareOneSequence(t *testing.T) {
	var mu sync.Mutex
	var seqs []uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, _ := readSeq(t, r)
		mu.Lock()
		seqs = append(seqs, seq)
		mu.Unlock()
		writeResult(w, seq, true)
	}))
	defer srv.Close()

	c := New(srv.URL)
	ctx := context.Background()
	if _, err := c.Put(ctx, "k", "old"); err != nil {
		t.Fatal(err)
	}
	index, swapped, err := c.CAS(ctx, "k", "old", "new")
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 || !swapped {
		t.Fatalf("cas = (%d, %v), want (2, true)", index, swapped)
	}
	index, err = c.Delete(ctx, "k")
	if err != nil {
		t.Fatal(err)
	}
	if index != 3 {
		t.Fatalf("delete index = %d, want 3", index)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(seqs) != 3 || seqs[0] != 1 || seqs[1] != 2 || seqs[2] != 3 {
		t.Fatalf("seqs = %v, want [1 2 3]", seqs)
	}
}

func TestConcurrentPutsStayContiguous(t *testing.T) {
	var mu sync.Mutex
	var last uint64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, _ := readSeq(t, r)
		mu.Lock()
		defer mu.Unlock()
		if seq != last && seq != last+1 {
			writeStatus(w, http.StatusBadRequest, "node: session")
			return
		}
		if seq == last+1 {
			last = seq
		}
		writeResult(w, seq, false)
	}))
	defer srv.Close()

	c := New(srv.URL)
	const n = 8
	errc := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := c.Put(context.Background(), "k", string(rune('a'+i)))
			errc <- err
		}(i)
	}
	wg.Wait()
	close(errc)
	for err := range errc {
		if err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if last != n {
		t.Fatalf("last seq = %d, want %d", last, n)
	}
}

type recorded struct {
	seq   uint64
	key   string
	value string
}

func lostResponseServer(t *testing.T) (*Client, *callLog) {
	t.Helper()
	log := &callLog{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seq, value := readSeq(t, r)
		key := r.PathValue("key")
		if key == "" {
			key = lastPath(r.URL.Path)
		}
		log.add(recorded{seq: seq, key: key, value: value})
		if log.len() == 1 {
			<-r.Context().Done()
			return
		}
		index := seq
		if key != "k" {
			index = 2
		} else if seq == 1 {
			index = 7
		}
		writeResult(w, index, false)
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL), log
}

func lastPath(path string) string {
	for i := len(path) - 1; i >= 0; i-- {
		if path[i] == '/' {
			return path[i+1:]
		}
	}
	return path
}

type callLog struct {
	mu    sync.Mutex
	items []recorded
}

func (c *callLog) add(item recorded) {
	c.mu.Lock()
	c.items = append(c.items, item)
	c.mu.Unlock()
}

func (c *callLog) len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.items)
}

func snapshot(c *callLog) []recorded {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]recorded, len(c.items))
	copy(out, c.items)
	return out
}

func waitForCall(t *testing.T, log *callLog, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if log.len() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("got %d calls, want %d", log.len(), n)
}

func readSeq(t *testing.T, r *http.Request) (uint64, string) {
	t.Helper()
	var body struct {
		Seq   uint64 `json:"seq"`
		Value string `json:"value"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Errorf("decode body: %v", err)
	}
	return body.Seq, body.Value
}

func writeResult(w http.ResponseWriter, index uint64, swapped bool) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Index   uint64 `json:"index"`
		Swapped bool   `json:"swapped"`
	}{Index: index, Swapped: swapped})
}

func writeStatus(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: msg})
}
