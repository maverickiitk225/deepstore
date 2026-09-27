package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestEnginePutGetReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Put("k", "v"); err != nil {
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
	if err := e.Put("k", "v"); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("k"); err != nil {
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

	if err := e.Put("", "v"); err == nil {
		t.Fatal("Put empty key: err = nil, want error")
	}
	if err := e.Delete(""); err == nil {
		t.Fatal("Delete empty key: err = nil, want error")
	}
}

func TestEngineTornTailAfterReopen(t *testing.T) {
	dir := t.TempDir()

	e, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.Put("ok", "1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Put("lost", "2"); err != nil {
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
			_ = e.Put("hot", val)
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
			errs[i] = e.Put(fmt.Sprintf("k%d", i), "v")
		}(i)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if err := e.Put("after", "x"); err == nil {
		t.Fatal("Put after Close: err = nil, want error")
	}
	if err := e.Delete("after"); err == nil {
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
	if err := e.Put("ok", "1"); err != nil {
		t.Fatal(err)
	}

	e.setSyncHook(func() error { return fmt.Errorf("disk failed") })

	if err := e.Put("lost", "2"); err == nil {
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

	err = e.Put("later", "3")
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("Put after poison: err = %v, want poisoned", err)
	}
	err = e.Delete("ok")
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
	if err := e.Put("a", "1"); err != nil {
		t.Fatal(err)
	}
	if err := e.Put("b", "2"); err != nil {
		t.Fatal(err)
	}
	if err := e.Delete("a"); err != nil {
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
