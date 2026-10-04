package store

import (
	"sync"
	"testing"

	"github.com/deepanker/deepstore/internal/snapshot"
)

func TestApplySetAndGet(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		value string
	}{
		{name: "single key", key: "k1", value: "v1"},
		{name: "empty value", key: "k2", value: ""},
		{name: "overwrite", key: "k1", value: "v2"},
	}

	sm := NewStateMachine()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := sm.Apply(Command{Type: CommandTypePut, Key: tt.key, Value: tt.value}); err != nil {
				t.Fatalf("Apply set: %v", err)
			}
			got, ok := sm.Get(tt.key)
			if !ok {
				t.Fatalf("Get(%q): ok = false, want true", tt.key)
			}
			if got != tt.value {
				t.Fatalf("Get(%q) = %q, want %q", tt.key, got, tt.value)
			}
		})
	}
}

func TestGetMissingKey(t *testing.T) {
	sm := NewStateMachine()
	_, ok := sm.Get("missing")
	if ok {
		t.Fatal("Get missing key: ok = true, want false")
	}
}

func TestApplyDelete(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := sm.Apply(Command{Type: CommandTypeDelete, Key: "a"}); err != nil {
		t.Fatal(err)
	}
	_, ok := sm.Get("a")
	if ok {
		t.Fatal("Get after delete: ok = true, want false")
	}
}

func TestApplyDeleteMissingKey(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(Command{Type: CommandTypeDelete, Key: "ghost"}); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
}

func TestApplyInvalidCommand(t *testing.T) {
	sm := NewStateMachine()
	_, err := sm.Apply(Command{Type: CommandType("nope"), Key: "k"})
	if err == nil {
		t.Fatal("Apply invalid type: err = nil, want error")
	}
}

func TestConcurrentSetSameKey(t *testing.T) {
	sm := NewStateMachine()
	const goroutines = 32
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				val := string(rune('a' + (g+i)%26))
				_, _ = sm.Apply(Command{Type: CommandTypePut, Key: "hot", Value: val})
			}
		}()
	}
	wg.Wait()

	got, ok := sm.Get("hot")
	if !ok {
		t.Fatal("Get hot: ok = false after concurrent sets")
	}
	if got == "" {
		t.Fatal("Get hot: empty value after concurrent sets")
	}
}

func TestApplyCAS(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "k", Value: "old"}); err != nil {
		t.Fatal(err)
	}

	res, err := sm.Apply(Command{Type: CommandTypeCAS, Key: "k", Expected: "stale", Value: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Swapped {
		t.Fatal("CAS stale expected: swapped = true, want false")
	}
	if got, ok := sm.Get("k"); !ok || got != "old" {
		t.Fatalf("Get after mismatch = (%q, %v), want (old, true)", got, ok)
	}

	res, err = sm.Apply(Command{Type: CommandTypeCAS, Key: "k", Expected: "old", Value: "new"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Swapped {
		t.Fatal("CAS matching expected: swapped = false, want true")
	}
	if got, ok := sm.Get("k"); !ok || got != "new" {
		t.Fatalf("Get after swap = (%q, %v), want (new, true)", got, ok)
	}

	res, err = sm.Apply(Command{Type: CommandTypeCAS, Key: "missing", Expected: "", Value: "x"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Swapped {
		t.Fatal("CAS missing key: swapped = true, want false")
	}
	if _, ok := sm.Get("missing"); ok {
		t.Fatal("Get missing after CAS: ok = true, want false")
	}

	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "empty", Value: ""}); err != nil {
		t.Fatal(err)
	}
	res, err = sm.Apply(Command{Type: CommandTypeCAS, Key: "empty", Expected: "", Value: "set"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.Swapped {
		t.Fatal("CAS empty value: swapped = false, want true")
	}
	if got, ok := sm.Get("empty"); !ok || got != "set" {
		t.Fatalf("Get(empty) = (%q, %v), want (set, true)", got, ok)
	}
}

func TestApplySession(t *testing.T) {
	sm := NewStateMachine()
	res, err := sm.Apply(Command{Type: CommandTypePut, Key: "k", Value: "v", ClientID: 7, Seq: 1, Index: 4})
	if err != nil {
		t.Fatal(err)
	}
	if res.Index != 4 {
		t.Fatalf("index = %d, want 4", res.Index)
	}
	res, err = sm.Apply(Command{Type: CommandTypePut, Key: "k", Value: "other", ClientID: 7, Seq: 1, Index: 9})
	if err != nil {
		t.Fatal(err)
	}
	if res.Index != 4 {
		t.Fatalf("retry index = %d, want 4", res.Index)
	}
	if got, ok := sm.Get("k"); !ok || got != "v" {
		t.Fatalf("Get = (%q, %v), want (v, true)", got, ok)
	}
	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "k", Value: "x", ClientID: 7, Seq: 3, Index: 10}); err == nil {
		t.Fatal("gap: err = nil, want error")
	}
	res, err = sm.Apply(Command{Type: CommandTypeCAS, Key: "k", Expected: "v", Value: "n", ClientID: 7, Seq: 2, Index: 5})
	if err != nil || !res.Swapped || res.Index != 5 {
		t.Fatalf("CAS = (%+v, %v), want swapped at 5", res, err)
	}
	res, err = sm.Apply(Command{Type: CommandTypeCAS, Key: "k", Expected: "nope", Value: "z", ClientID: 8, Seq: 1, Index: 6})
	if err != nil || res.Swapped {
		t.Fatalf("other client CAS = (%+v, %v), want not swapped", res, err)
	}
	res, err = sm.Apply(Command{Type: CommandTypePut, Key: "k", Value: "nope", ClientID: 9, Seq: 1, Index: 7})
	if err != nil {
		t.Fatal(err)
	}
	res, err = sm.Apply(Command{Type: CommandTypeCAS, Key: "k", Expected: "nope", Value: "z", ClientID: 8, Seq: 1, Index: 99})
	if err != nil || res.Swapped || res.Index != 6 {
		t.Fatalf("CAS retry = (%+v, %v), want index 6 not swapped", res, err)
	}
	if got, ok := sm.Get("k"); !ok || got != "nope" {
		t.Fatalf("Get = (%q, %v), want (nope, true)", got, ok)
	}
}

func TestExportRestore(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "k", Value: "v", ClientID: 4, Seq: 1, Index: 3}); err != nil {
		t.Fatal(err)
	}
	res, err := sm.Apply(Command{Type: CommandTypeCAS, Key: "k", Expected: "v", Value: "n", ClientID: 4, Seq: 2, Index: 4})
	if err != nil || !res.Swapped {
		t.Fatalf("CAS = (%+v, %v)", res, err)
	}
	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "empty", Value: "", ClientID: 5, Seq: 1, Index: 5}); err != nil {
		t.Fatal(err)
	}

	data, sessions := sm.Export()
	data["extra"] = "x"
	if _, ok := sm.Get("extra"); ok {
		t.Fatal("export aliased the map")
	}

	snapSessions := make(map[uint64]snapshot.Session, len(sessions))
	for id, sess := range sessions {
		snapSessions[id] = snapshot.Session{Seq: sess.Seq, Index: sess.Index, Swapped: sess.Swapped}
	}
	restored := NewStateMachine()
	restored.Restore(snapshot.Snapshot{Index: 5, Data: data, Sessions: snapSessions})
	if got, ok := restored.Get("k"); !ok || got != "n" {
		t.Fatalf("Get(k) = (%q, %v), want (n, true)", got, ok)
	}
	if got, ok := restored.Get("empty"); !ok || got != "" {
		t.Fatalf("Get(empty) = (%q, %v), want (\"\", true)", got, ok)
	}
	res, err = restored.Apply(Command{Type: CommandTypePut, Key: "k", Value: "other", ClientID: 4, Seq: 2, Index: 9})
	if err != nil || res.Index != 4 || !res.Swapped {
		t.Fatalf("retry = (%+v, %v), want index 4 swapped", res, err)
	}
	if _, err := restored.Apply(Command{Type: CommandTypePut, Key: "k", Value: "next", ClientID: 4, Seq: 3, Index: 6}); err != nil {
		t.Fatal(err)
	}
	if got, ok := sm.Get("k"); !ok || got != "n" {
		t.Fatalf("original Get(k) = (%q, %v), want (n, true)", got, ok)
	}
}

func TestGetConcurrentWithSet(t *testing.T) {
	sm := NewStateMachine()
	if _, err := sm.Apply(Command{Type: CommandTypePut, Key: "x", Value: "start"}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = sm.Apply(Command{Type: CommandTypePut, Key: "x", Value: "v"})
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_, _ = sm.Get("x")
		}
	}()
	wg.Wait()
}
