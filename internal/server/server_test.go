package server

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/deepanker/deepstore/internal/client"
	"github.com/deepanker/deepstore/internal/engine"
)

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
	s := New(e, ln.Addr().String())
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
	s := New(e, ln.Addr().String())
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
