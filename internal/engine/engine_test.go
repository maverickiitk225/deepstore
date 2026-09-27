package engine

import (
	"os"
	"path/filepath"
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
