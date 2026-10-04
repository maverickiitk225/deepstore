package engine

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/snapshot"
	"github.com/deepanker/deepstore/internal/wal"
)

var freshClient atomic.Uint64

// put1, del1, and cas1 are separate clients, each sending sequence 1.
func put1(e *Engine, key, value string) (uint64, error) {
	return e.Put(key, value, freshClient.Add(1), 1)
}

func del1(e *Engine, key string) (uint64, error) {
	return e.Delete(key, freshClient.Add(1), 1)
}

func cas1(e *Engine, key, expected, value string) (uint64, bool, error) {
	return e.CAS(key, expected, value, freshClient.Add(1), 1)
}

func TestEnginePutGetReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "k", "v"); err != nil {
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
	if _, err := put1(e, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := del1(e, "k"); err != nil {
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

func TestEngineCAS(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	index, err := put1(e, "k", "old")
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("Put index = %d, want 1", index)
	}

	index, swapped, err := cas1(e, "k", "stale", "nope")
	if err != nil {
		t.Fatal(err)
	}
	if index != 2 || swapped {
		t.Fatalf("CAS mismatch = (%d, %v), want (2, false)", index, swapped)
	}
	if got, ok := e.Get("k"); !ok || got != "old" {
		t.Fatalf("Get after mismatch = (%q, %v), want (old, true)", got, ok)
	}

	index, swapped, err = cas1(e, "k", "old", "new")
	if err != nil {
		t.Fatal(err)
	}
	if index != 3 || !swapped {
		t.Fatalf("CAS match = (%d, %v), want (3, true)", index, swapped)
	}

	index, swapped, err = cas1(e, "missing", "", "x")
	if err != nil {
		t.Fatal(err)
	}
	if index != 4 || swapped {
		t.Fatalf("CAS missing = (%d, %v), want (4, false)", index, swapped)
	}
	if _, ok := e.Get("missing"); ok {
		t.Fatal("Get(missing) after CAS: ok = true, want false")
	}
	if _, _, err := cas1(e, "", "a", "b"); err == nil {
		t.Fatal("CAS empty key: err = nil, want error")
	}

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got, ok := e.Get("k"); !ok || got != "new" {
		t.Fatalf("Get after reopen = (%q, %v), want (new, true)", got, ok)
	}
	if e.LastIndex() != 4 || e.AppliedIndex() != 4 {
		t.Fatalf("indexes after reopen = (%d, %d), want (4, 4)", e.LastIndex(), e.AppliedIndex())
	}
}

func TestEngineCASSeesEarlierRecordInBatch(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.mu.Lock()
	e.holdBatch = true
	e.mu.Unlock()

	putDone := make(chan struct{})
	var putIndex uint64
	var putErr error
	go func() {
		defer close(putDone)
		putIndex, putErr = put1(e, "k", "old")
	}()
	waitQueued(t, e, 1)

	casDone := make(chan struct{})
	var casIndex uint64
	var swapped bool
	var casErr error
	go func() {
		defer close(casDone)
		casIndex, swapped, casErr = cas1(e, "k", "old", "new")
	}()
	waitQueued(t, e, 2)

	e.mu.Lock()
	e.holdBatch = false
	e.cond.Broadcast()
	e.mu.Unlock()
	<-putDone
	<-casDone

	if putErr != nil || casErr != nil {
		t.Fatalf("put err %v, cas err %v", putErr, casErr)
	}
	if putIndex != 1 || casIndex != 2 || !swapped {
		t.Fatalf("put index %d, cas (%d, %v), want 1 and (2, true)", putIndex, casIndex, swapped)
	}
	if got, ok := e.Get("k"); !ok || got != "new" {
		t.Fatalf("Get(k) = (%q, %v), want (new, true)", got, ok)
	}
}

func TestEngineCASOneWinner(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := put1(e, "k", "old"); err != nil {
		t.Fatal(err)
	}

	const n = 32
	var wg sync.WaitGroup
	wg.Add(n)
	wins := make([]bool, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			_, wins[i], errs[i] = cas1(e, "k", "old", fmt.Sprintf("w%d", i))
		}(i)
	}
	wg.Wait()

	won := 0
	var winner string
	for i := range errs {
		if errs[i] != nil {
			t.Fatalf("CAS %d: %v", i, errs[i])
		}
		if wins[i] {
			won++
			winner = fmt.Sprintf("w%d", i)
		}
	}
	if won != 1 {
		t.Fatalf("winners = %d, want 1", won)
	}
	if got, ok := e.Get("k"); !ok || got != winner {
		t.Fatalf("Get(k) = (%q, %v), want (%q, true)", got, ok, winner)
	}
}

func TestEngineEmptyKey(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	if _, err := put1(e, "", "v"); err == nil {
		t.Fatal("Put empty key: err = nil, want error")
	}
	if _, err := del1(e, ""); err == nil {
		t.Fatal("Delete empty key: err = nil, want error")
	}
}

func TestEngineTornTailAfterReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "ok", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "lost", "2"); err != nil {
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
			_, _ = put1(e, "hot", val)
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
			_, errs[i] = put1(e, fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "after", "x"); err == nil {
		t.Fatal("Put after Close: err = nil, want error")
	}
	if _, err := del1(e, "after"); err == nil {
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
	if _, err := put1(e, "ok", "1"); err != nil {
		t.Fatal(err)
	}

	e.setSyncHook(func() error { return fmt.Errorf("disk failed") })

	if _, err := put1(e, "lost", "2"); err == nil {
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

	_, err = put1(e, "later", "3")
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("Put after poison: err = %v, want poisoned", err)
	}
	_, err = del1(e, "ok")
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
	if _, err := put1(e, "a", "1"); err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "b", "2"); err != nil {
		t.Fatal(err)
	}
	if _, err := del1(e, "a"); err != nil {
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
			idxs[i], errs[i] = put1(e, fmt.Sprintf("k%d", i), "v")
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
			_, errs[i] = put1(e, fmt.Sprintf("k%d", i), "v")
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
	if _, err := put1(e, "later", "x"); err == nil || !strings.Contains(err.Error(), "poisoned") {
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
		_, err := put1(e, "a", "1")
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
			_, errs[i] = put1(e, fmt.Sprintf("k%d", i), "v")
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
			if _, err := put1(e, fmt.Sprintf("k%d", id), "v"); err != nil {
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
	if _, err := put1(e, "ok", "1"); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "wal.log")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := e.commit(wal.Record{OpType: "bogus", Key: "k"}); err == nil {
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
	if _, err := put1(e, "next", "2"); err != nil {
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

func TestEngineSessionRetry(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	index, err := e.Put("k", "v", 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if index != 1 {
		t.Fatalf("first index = %d, want 1", index)
	}
	path := filepath.Join(dir, "wal.log")
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	again, err := e.Put("k", "other", 4, 1)
	if err != nil {
		t.Fatal(err)
	}
	if again != index {
		t.Fatalf("retry index = %d, want %d", again, index)
	}
	if got, ok := e.Get("k"); !ok || got != "v" {
		t.Fatalf("Get after retry = (%q, %v), want (v, true)", got, ok)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("wal size = %d, want %d", after.Size(), before.Size())
	}
	if _, err := e.Put("k", "skip", 4, 3); !errors.Is(err, errs.ErrSession) {
		t.Fatalf("gap err = %v, want session", err)
	}
	if e.LastIndex() != 1 {
		t.Fatalf("LastIndex after gap = %d, want 1", e.LastIndex())
	}
	index, err = e.Put("k", "v2", 4, 2)
	if err != nil || index != 2 {
		t.Fatalf("seq 2 = (%d, %v), want (2, nil)", index, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	again, err = e.Put("k", "ignored", 4, 2)
	if err != nil || again != 2 {
		t.Fatalf("retry after reopen = (%d, %v), want (2, nil)", again, err)
	}
	if got, ok := e.Get("k"); !ok || got != "v2" {
		t.Fatalf("Get after reopen = (%q, %v), want (v2, true)", got, ok)
	}
	if _, err := e.Put("k", "old", 4, 1); !errors.Is(err, errs.ErrSession) {
		t.Fatalf("old seq err = %v, want session", err)
	}
}

func TestEngineSessionCASRetryAfterOtherWrite(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Put("k", "old", 1, 1); err != nil {
		t.Fatal(err)
	}
	index, swapped, err := e.CAS("k", "old", "ada", 2, 1)
	if err != nil || !swapped || index != 2 {
		t.Fatalf("CAS = (%d, %v, %v), want (2, true, nil)", index, swapped, err)
	}
	if _, err := e.Put("k", "grace", 3, 1); err != nil {
		t.Fatal(err)
	}
	again, swapped, err := e.CAS("k", "old", "ada", 2, 1)
	if err != nil || !swapped || again != index {
		t.Fatalf("CAS retry = (%d, %v, %v), want (%d, true, nil)", again, swapped, err, index)
	}
	if got, ok := e.Get("k"); !ok || got != "grace" {
		t.Fatalf("Get = (%q, %v), want (grace, true)", got, ok)
	}
}

func TestEngineSessionFailedCASStaysFailed(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if _, err := e.Put("k", "old", 1, 1); err != nil {
		t.Fatal(err)
	}
	index, swapped, err := e.CAS("k", "stale", "nope", 2, 1)
	if err != nil || swapped {
		t.Fatalf("CAS miss = (%d, %v, %v), want swapped false", index, swapped, err)
	}
	if _, err := e.Put("k", "stale", 3, 1); err != nil {
		t.Fatal(err)
	}
	again, swapped, err := e.CAS("k", "stale", "nope", 2, 1)
	if err != nil || swapped || again != index {
		t.Fatalf("CAS retry = (%d, %v, %v), want (%d, false, nil)", again, swapped, err, index)
	}
	if got, ok := e.Get("k"); !ok || got != "stale" {
		t.Fatalf("Get = (%q, %v), want (stale, true)", got, ok)
	}
}

func TestEngineSessionDuplicateInBatch(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	e.mu.Lock()
	e.holdBatch = true
	e.mu.Unlock()

	firstDone := make(chan struct{})
	var first uint64
	var firstErr error
	go func() {
		defer close(firstDone)
		first, firstErr = e.Put("k", "first", 9, 1)
	}()
	waitQueued(t, e, 1)
	secondDone := make(chan struct{})
	var second uint64
	var secondErr error
	go func() {
		defer close(secondDone)
		second, secondErr = e.Put("k", "second", 9, 1)
	}()
	waitQueued(t, e, 2)

	e.mu.Lock()
	e.holdBatch = false
	e.cond.Broadcast()
	e.mu.Unlock()
	<-firstDone
	<-secondDone
	if firstErr != nil || secondErr != nil {
		t.Fatalf("first %v, second %v", firstErr, secondErr)
	}
	if first != 1 || second != 1 {
		t.Fatalf("indexes = (%d, %d), want (1, 1)", first, second)
	}
	if got, ok := e.Get("k"); !ok || got != "first" {
		t.Fatalf("Get = (%q, %v), want (first, true)", got, ok)
	}
	if e.LastIndex() != 1 {
		t.Fatalf("LastIndex = %d, want 1", e.LastIndex())
	}
}

func TestEngineReplaysUnsessionedRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := wal.Open(dir, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Append(wal.Record{Index: 1, OpType: wal.OpTypePut, Key: "k", Value: "v"}); err != nil {
		t.Fatal(err)
	}
	if err := w.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got, ok := e.Get("k"); !ok || got != "v" {
		t.Fatalf("Get = (%q, %v), want (v, true)", got, ok)
	}
	if _, err := e.Put("k", "v2", 3, 1); err != nil {
		t.Fatal(err)
	}
	if got, ok := e.Get("k"); !ok || got != "v2" {
		t.Fatalf("Get after sessioned put = (%q, %v), want (v2, true)", got, ok)
	}
}

func TestEngineSnapshotReopen(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e.SetSnapshotEvery(0)
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "snapshot")); !os.IsNotExist(err) {
		t.Fatalf("empty snapshot stat err = %v, want not exist", err)
	}

	if _, err := put1(e, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "empty", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := put1(e, "gone", "x"); err != nil {
		t.Fatal(err)
	}
	if _, err := del1(e, "gone"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	before := info.Size()
	if err := e.Snapshot(); err != nil {
		t.Fatal(err)
	}
	if e.SnapshotIndex() != 4 || e.LastIndex() != 4 || e.AppliedIndex() != 4 {
		t.Fatalf("indexes = (%d, %d, %d), want (4, 4, 4)", e.SnapshotIndex(), e.LastIndex(), e.AppliedIndex())
	}
	info, err = os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != before {
		t.Fatalf("wal size = %d, want %d", info.Size(), before)
	}

	index, err := put1(e, "tail", "yes")
	if err != nil || index != 5 {
		t.Fatalf("tail put = (%d, %v), want (5, nil)", index, err)
	}
	info, err = os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("wal size = 0, want the tail record")
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got, ok := e.Get("k"); !ok || got != "v" {
		t.Fatalf("Get(k) = (%q, %v), want (v, true)", got, ok)
	}
	if got, ok := e.Get("empty"); !ok || got != "" {
		t.Fatalf("Get(empty) = (%q, %v), want (\"\", true)", got, ok)
	}
	if _, ok := e.Get("gone"); ok {
		t.Fatal("Get(gone) after snapshot: ok = true, want false")
	}
	if got, ok := e.Get("tail"); !ok || got != "yes" {
		t.Fatalf("Get(tail) = (%q, %v), want (yes, true)", got, ok)
	}
	if e.LastIndex() != 5 || e.AppliedIndex() != 5 || e.SnapshotIndex() != 4 {
		t.Fatalf("indexes after reopen = (%d, %d, %d), want (5, 5, 4)", e.LastIndex(), e.AppliedIndex(), e.SnapshotIndex())
	}
}

func TestEngineSnapshotSessionAndCAS(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e.SetSnapshotEvery(0)
	if _, err := e.Put("k", "old", 4, 1); err != nil {
		t.Fatal(err)
	}
	index, swapped, err := e.CAS("k", "stale", "nope", 5, 1)
	if err != nil || swapped || index != 2 {
		t.Fatalf("CAS miss = (%d, %v, %v), want (2, false, nil)", index, swapped, err)
	}
	okIndex, swapped, err := e.CAS("k", "old", "new", 6, 1)
	if err != nil || !swapped || okIndex != 3 {
		t.Fatalf("CAS hit = (%d, %v, %v), want (3, true, nil)", okIndex, swapped, err)
	}
	if err := e.Snapshot(); err != nil {
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
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("wal size = 0, want the log kept")
	}
	walSize := info.Size()
	again, swapped, err := e.CAS("k", "stale", "nope", 5, 1)
	if err != nil || swapped || again != index {
		t.Fatalf("CAS retry = (%d, %v, %v), want (%d, false, nil)", again, swapped, err, index)
	}
	again, swapped, err = e.CAS("k", "old", "new", 6, 1)
	if err != nil || !swapped || again != okIndex {
		t.Fatalf("CAS hit retry = (%d, %v, %v), want (%d, true, nil)", again, swapped, err, okIndex)
	}
	if got, ok := e.Get("k"); !ok || got != "new" {
		t.Fatalf("Get(k) = (%q, %v), want (new, true)", got, ok)
	}
	info, err = os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != walSize {
		t.Fatalf("wal size after retries = %d, want %d", info.Size(), walSize)
	}
	next, err := e.Put("k", "later", 4, 2)
	if err != nil || next != 4 {
		t.Fatalf("next put = (%d, %v), want (4, nil)", next, err)
	}
}

func TestEngineOpenAppliesTailPastSnapshot(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e.SetSnapshotEvery(0)
	const id = uint64(4)
	idx1, err := e.Put("k", "v1", id, 1)
	if err != nil || idx1 != 1 {
		t.Fatalf("first = (%d, %v)", idx1, err)
	}
	idx2, err := e.Put("k", "v2", id, 2)
	if err != nil || idx2 != 2 {
		t.Fatalf("second = (%d, %v)", idx2, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}

	if err := snapshot.Save(dir, snapshot.Snapshot{
		Index: idx1,
		Data:  map[string]string{"k": "v1"},
		Sessions: map[uint64]snapshot.Session{
			id: {Seq: 1, Index: idx1},
		},
	}); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got, ok := e.Get("k"); !ok || got != "v2" {
		t.Fatalf("Get(k) = (%q, %v), want (v2, true)", got, ok)
	}
	after, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if after.Size() != before.Size() {
		t.Fatalf("wal size = %d, want %d", after.Size(), before.Size())
	}
	again, err := e.Put("k", "ignored", id, 2)
	if err != nil || again != idx2 {
		t.Fatalf("retry = (%d, %v), want (%d, nil)", again, err, idx2)
	}
	next, err := e.Put("k", "v3", id, 3)
	if err != nil || next != 3 {
		t.Fatalf("next = (%d, %v), want (3, nil)", next, err)
	}
}

func TestEngineOpenSkipsPrefixCoveredBySnapshot(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e.SetSnapshotEvery(0)
	const id = uint64(4)
	if _, err := e.Put("k", "v1", id, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("k", "v2", id, 2); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := snapshot.Save(dir, snapshot.Snapshot{
		Index: 2,
		Data:  map[string]string{"k": "v2"},
		Sessions: map[uint64]snapshot.Session{
			id: {Seq: 2, Index: 2},
		},
	}); err != nil {
		t.Fatal(err)
	}

	e, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if got, ok := e.Get("k"); !ok || got != "v2" {
		t.Fatalf("Get(k) = (%q, %v), want (v2, true)", got, ok)
	}
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("wal size = 0, want the log kept")
	}
	again, err := e.Put("k", "ignored", id, 2)
	if err != nil || again != 2 {
		t.Fatalf("retry = (%d, %v), want (2, nil)", again, err)
	}
}

func TestEngineSnapshotEvery(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	e.SetSnapshotEvery(2)

	if _, err := e.Put("a", "1", 1, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("b", "2", 2, 1); err != nil {
		t.Fatal(err)
	}
	// The snapshot runs after the ack, so a later command waits until it finishes.
	if _, err := e.Put("a", "1", 1, 1); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() == 0 {
		t.Fatal("wal size after first snapshot = 0, want the log kept")
	}
	firstSize := info.Size()
	if e.SnapshotIndex() != 2 {
		t.Fatalf("snapshot index = %d, want 2", e.SnapshotIndex())
	}

	if _, err := e.Put("c", "3", 3, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("d", "4", 4, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Put("a", "1", 1, 1); err != nil {
		t.Fatal(err)
	}
	info, err = os.Stat(filepath.Join(dir, "wal.log"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() <= firstSize {
		t.Fatalf("wal size after later puts = %d, want above %d", info.Size(), firstSize)
	}
	if e.SnapshotIndex() != 4 || e.LastIndex() != 4 {
		t.Fatalf("indexes = (%d, %d), want (4, 4)", e.SnapshotIndex(), e.LastIndex())
	}
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3", "d": "4"} {
		got, ok := e.Get(key)
		if !ok || got != want {
			t.Fatalf("Get(%s) = (%q, %v), want (%s, true)", key, got, ok, want)
		}
	}
}

func TestEngineSnapshotClosed(t *testing.T) {
	e, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Snapshot(); err == nil {
		t.Fatal("Snapshot after Close: err = nil, want error")
	}
}

func TestEngineCorruptSnapshotFailsOpen(t *testing.T) {
	dir := t.TempDir()
	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	e.SetSnapshotEvery(0)
	if _, err := put1(e, "k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "snapshot"), []byte("not a snapshot"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err == nil {
		t.Fatal("Open corrupt snapshot: err = nil, want error")
	}
}
