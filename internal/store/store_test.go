package store

import (
	"sync"
	"testing"
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
			if err := sm.Apply(Command{Type: CommandTypeSet, Key: tt.key, Value: tt.value}); err != nil {
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
	if err := sm.Apply(Command{Type: CommandTypeSet, Key: "a", Value: "1"}); err != nil {
		t.Fatal(err)
	}
	if err := sm.Apply(Command{Type: CommandTypeDelete, Key: "a"}); err != nil {
		t.Fatal(err)
	}
	_, ok := sm.Get("a")
	if ok {
		t.Fatal("Get after delete: ok = true, want false")
	}
}

func TestApplyDeleteMissingKey(t *testing.T) {
	sm := NewStateMachine()
	if err := sm.Apply(Command{Type: CommandTypeDelete, Key: "ghost"}); err != nil {
		t.Fatalf("delete missing key: %v", err)
	}
}

func TestApplyClear(t *testing.T) {
	sm := NewStateMachine()
	for _, kv := range []struct{ k, v string }{{"a", "1"}, {"b", "2"}} {
		if err := sm.Apply(Command{Type: CommandTypeSet, Key: kv.k, Value: kv.v}); err != nil {
			t.Fatal(err)
		}
	}
	if err := sm.Apply(Command{Type: CommandTypeClear}); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"a", "b"} {
		if _, ok := sm.Get(key); ok {
			t.Fatalf("Get(%q) after clear: ok = true, want false", key)
		}
	}
}

func TestApplyInvalidCommand(t *testing.T) {
	sm := NewStateMachine()
	err := sm.Apply(Command{Type: CommandType("nope"), Key: "k"})
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
				_ = sm.Apply(Command{Type: CommandTypeSet, Key: "hot", Value: val})
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

func TestGetConcurrentWithSet(t *testing.T) {
	sm := NewStateMachine()
	if err := sm.Apply(Command{Type: CommandTypeSet, Key: "x", Value: "start"}); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			_ = sm.Apply(Command{Type: CommandTypeSet, Key: "x", Value: "v"})
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
