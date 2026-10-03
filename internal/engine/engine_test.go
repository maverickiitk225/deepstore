package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/wal"
)

func TestEnginePutGetReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	got, ok := e.Get("k")
	if !ok || got != "v" {
		t.Fatalf("Get(k) = (%q, %v), want (v, true)", got, ok)
	}
}

func TestEngineDeleteReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Delete("k"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if _, ok := e.Get("k"); ok {
		t.Fatal("Get(k) after delete reopen: ok = true, want false")
	}
}

func TestEngineEmptyKey(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if _, err := e.Put("", "v"); err == nil {
		t.Fatal("Put empty key: err = nil, want error")
	}
	if _, err := e.Delete(""); err == nil {
		t.Fatal("Delete empty key: err = nil, want error")
	}
}

func TestEngineTornTailAfterReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("ok", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("lost", "2"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "wal.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-3); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if v, ok := e.Get("ok"); !ok || v != "1" {
		t.Fatalf("Get(ok) = (%q, %v), want (1, true)", v, ok)
	}
	if _, ok := e.Get("lost"); ok {
		t.Fatal("Get(lost) after torn tail: ok = true, want false")
	}
}

func TestEngineConcurrentPutSameKey(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	const n = 16
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			val := string(rune('a' + i%26))
			_, _ = e.Put("hot", val)
		}(i)
	}
	for i := 0; i < n; i++ {
		<-done
	}

	got, ok := e.Get("hot")
	if !ok || got == "" {
		t.Fatalf("Get(hot) = (%q, %v), want non-empty value", got, ok)
	}
}

func TestEngineCloseWaitsForInFlightPuts(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	const n = 64
	errs := make([]error, n)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = e.Put(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("after", "x"); err == nil {
		t.Fatal("Put after Close: err = nil, want error")
	}
	if _, err := e.Delete("after"); err == nil {
		t.Fatal("Delete after Close: err = nil, want error")
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	for i, putErr := range errs {
		key := fmt.Sprintf("k%d", i)
		_, ok := e.Get(key)
		if putErr == nil && !ok {
			t.Fatalf("Put(%s) returned nil but key missing after reopen", key)
		}
		if putErr != nil && ok {
			t.Fatalf("Put(%s) returned %v but key is present after reopen", key, putErr)
		}
	}
	if _, ok := e.Get("after"); ok {
		t.Fatal("Put after Close was persisted")
	}
}

func TestEngineSyncFailurePoisons(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("ok", "1"); err != nil {
		t.Fatal(err)
	}

	e.setSyncHook(func() error { return fmt.Errorf("disk failed") })

	if _, err := e.Put("lost", "2"); err == nil {
		t.Fatal("Put during sync failure: err = nil, want error")
	}
	if _, ok := e.Get("lost"); ok {
		t.Fatal("Get(lost) after failed sync: ok = true, want false")
	}
	if e.LastIndex() != 1 || e.AppliedIndex() != 1 {
		t.Fatalf("indexes after failed sync = (%d, %d), want (1, 1)", e.LastIndex(), e.AppliedIndex())
	}
	if v, ok := e.Get("ok"); !ok || v != "1" {
		t.Fatalf("Get(ok) = (%q, %v), want (1, true)", v, ok)
	}

	path := filepath.Join(dir, "wal.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	sizeAfterFailure := info.Size()

	_, err = e.Put("later", "3")
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("Put after poison: err = %v, want poisoned", err)
	}
	_, err = e.Delete("ok")
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("Delete after poison: err = %v, want poisoned", err)
	}

	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != sizeAfterFailure {
		t.Fatalf("wal size = %d, want %d", info.Size(), sizeAfterFailure)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if v, ok := e.Get("ok"); !ok || v != "1" {
		t.Fatalf("Get(ok) after reopen = (%q, %v), want (1, true)", v, ok)
	}
	if _, ok := e.Get("later"); ok {
		t.Fatal("Get(later) after reopen: ok = true, want false")
	}
	if v, ok := e.Get("lost"); !ok || v != "2" {
		t.Fatalf("Get(lost) after reopen = (%q, %v), want (2, true)", v, ok)
	}
}

func TestEngineIndexMonotonic(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if e.LastIndex() != 0 || e.AppliedIndex() != 0 {
		t.Fatalf("indexes on empty engine = (%d, %d), want (0, 0)", e.LastIndex(), e.AppliedIndex())
	}
	if _, err := e.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("b", "2"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Delete("a"); err != nil {
		t.Fatal(err)
	}
	if e.LastIndex() != 3 || e.AppliedIndex() != 3 {
		t.Fatalf("indexes after three writes = (%d, %d), want (3, 3)", e.LastIndex(), e.AppliedIndex())
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if e.LastIndex() != 3 || e.AppliedIndex() != 3 {
		t.Fatalf("indexes after reopen = (%d, %d), want (3, 3)", e.LastIndex(), e.AppliedIndex())
	}
	if _, ok := e.Get("a"); ok {
		t.Fatal("Get(a) after delete reopen: ok = true, want false")
	}
	if v, ok := e.Get("b"); !ok || v != "2" {
		t.Fatalf("Get(b) = (%q, %v), want (2, true)", v, ok)
	}
}

func TestEngineGroupCommitOneSync(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.mu.Lock()
	e.holdBatch = true
	e.mu.Unlock()

	var syncs atomic.Int32
	e.setSyncHook(func() error {
		syncs.Add(1)
		return nil
	})

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	idxs := make([]uint64, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			idxs[i], errs[i] = e.Put(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	waitQueued(t, e, n)

	e.mu.Lock()
	e.holdBatch = false
	e.cond.Broadcast()
	e.mu.Unlock()
	wg.Wait()

	if syncs.Load() != 1 {
		t.Fatalf("syncs = %d, want 1", syncs.Load())
	}
	seen := make(map[uint64]bool, n)
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("Put k%d: %v", i, errs[i])
		}
		if idxs[i] < 1 || idxs[i] > n || seen[idxs[i]] {
			t.Fatalf("Put k%d index = %d, want each of 1..%d once", i, idxs[i], n)
		}
		seen[idxs[i]] = true
		got, ok := e.Get(fmt.Sprintf("k%d", i))
		if !ok || got != "v" {
			t.Fatalf("Get(k%d) = (%q, %v), want (v, true)", i, got, ok)
		}
	}
	if e.LastIndex() != n || e.AppliedIndex() != n {
		t.Fatalf("indexes = (%d, %d), want (%d, %d)", e.LastIndex(), e.AppliedIndex(), n, n)
	}
}

func TestEngineGroupCommitSyncFailureAcksNobody(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.mu.Lock()
	e.holdBatch = true
	e.mu.Unlock()
	e.setSyncHook(func() error { return fmt.Errorf("disk failed") })

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = e.Put(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	waitQueued(t, e, n)

	e.mu.Lock()
	e.holdBatch = false
	e.cond.Broadcast()
	e.mu.Unlock()
	wg.Wait()

	for i := range errs {
		if errs[i] == nil {
			t.Fatalf("Put k%d returned nil, want error", i)
		}
		if _, ok := e.Get(fmt.Sprintf("k%d", i)); ok {
			t.Fatalf("Get(k%d) visible after failed sync", i)
		}
	}
	if e.LastIndex() != 0 || e.AppliedIndex() != 0 {
		t.Fatalf("indexes after failed batch = (%d, %d), want (0, 0)", e.LastIndex(), e.AppliedIndex())
	}

	path := filepath.Join(dir, "wal.log")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	sizeAfterFailure := info.Size()
	if _, err := e.Put("later", "x"); err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("Put after poison: err = %v, want poisoned", err)
	}
	info, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != sizeAfterFailure {
		t.Fatalf("wal size = %d, want %d", info.Size(), sizeAfterFailure)
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	for i := 0; i < n; i++ {
		got, ok := e.Get(fmt.Sprintf("k%d", i))
		if !ok || got != "v" {
			t.Fatalf("Get(k%d) after reopen = (%q, %v), want the whole batch", i, got, ok)
		}
	}
	if _, ok := e.Get("later"); ok {
		t.Fatal("Get(later) after reopen: ok = true, want false")
	}
}

func TestEngineQueuesWhileSyncing(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}

	releaseSync := make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(releaseSync) }) }
	defer e.Close()
	defer release()

	e.mu.Lock()
	e.holdBatch = true
	e.mu.Unlock()

	entered := make(chan struct{})
	var syncs atomic.Int32
	e.setSyncHook(func() error {
		if syncs.Add(1) == 1 {
			close(entered)
			<-releaseSync
		}
		return nil
	})

	firstDone := make(chan error, 1)
	go func() {
		_, err := e.Put("a", "1")
		firstDone <- err
	}()
	waitQueued(t, e, 1)
	e.mu.Lock()
	e.holdBatch = false
	e.cond.Broadcast()
	e.mu.Unlock()
	<-entered

	const extra = 4
	var wg sync.WaitGroup
	wg.Add(extra)
	errs := make([]error, extra)
	for i := 0; i < extra; i++ {
		go func(i int) {
			defer wg.Done()
			_, errs[i] = e.Put(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	waitQueued(t, e, extra)
	if syncs.Load() != 1 {
		t.Fatalf("syncs while second batch is queued = %d, want 1", syncs.Load())
	}
	release()
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("Put k%d: %v", i, err)
		}
	}
	if syncs.Load() != 2 {
		t.Fatalf("syncs = %d, want 2", syncs.Load())
	}
	if e.LastIndex() != 1+extra || e.AppliedIndex() != 1+extra {
		t.Fatalf("indexes = (%d, %d), want %d", e.LastIndex(), e.AppliedIndex(), 1+extra)
	}
}

func BenchmarkEngineConcurrentPut(b *testing.B) {
	e, err := Open(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close()

	var seq atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			id := seq.Add(1)
			if _, err := e.Put(fmt.Sprintf("k%d", id), "v"); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func waitQueued(t *testing.T, e *Engine, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		e.mu.Lock()
		got := len(e.queue)
		e.mu.Unlock()
		if got == n {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("queued = %d, want %d", got, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestEngineRejectsInvalidOpBeforeWAL(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("ok", "1"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "wal.log")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := e.commit(wal.Record{OpType: "bogus", Key: "k"}); err == nil {
		t.Fatal("commit(bogus): err = nil, want error")
	}

	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("wal size = %d, want %d", after.Size(), before.Size())
	}
	if e.LastIndex() != 1 {
		t.Fatalf("LastIndex = %d, want 1", e.LastIndex())
	}
	if _, err := e.Put("next", "2"); err != nil {
		t.Fatalf("Put after rejected op: %v", err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer e.Close()
	if v, ok := e.Get("next"); !ok || v != "2" {
		t.Fatalf("Get(next) after reopen = (%q, %v), want (2, true)", v, ok)
	}
}
