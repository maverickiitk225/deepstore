package engine

import (
	"fmt"
	"sync"

	"github.com/deepanker/deepstore/internal/store"
	"github.com/deepanker/deepstore/internal/wal"
)

type Engine struct {
	wal          *wal.WAL
	sm           *store.StateMachine
	mu           sync.Mutex
	closed       bool
	poisoned     error
	lastIndex    uint64
	appliedIndex uint64
}

func Open(dir string) (*Engine, error) {
	sm := store.NewStateMachine()
	w, err := wal.Open(dir, func(r wal.Record) error {
		cmd, err := recordToCommand(r)
		if err != nil {
			return err
		}
		return sm.Apply(cmd)
	})
	if err != nil {
		return nil, err
	}
	return &Engine{
		wal:          w,
		sm:           sm,
		lastIndex:    w.LastIndex(),
		appliedIndex: w.LastIndex(),
	}, nil
}

func (e *Engine) Put(key, value string) error {
	if key == "" {
		return fmt.Errorf("engine: empty key")
	}
	return e.commit(wal.Record{OpType: wal.OpTypePut, Key: key, Value: value})
}

func (e *Engine) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("engine: empty key")
	}
	return e.commit(wal.Record{OpType: wal.OpTypeDelete, Key: key})
}

func (e *Engine) commit(rec wal.Record) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.errIfNotWritable(); err != nil {
		return err
	}

	rec.Index = e.lastIndex + 1
	if err := e.wal.Append(rec); err != nil {
		return err
	}
	if err := e.wal.Sync(); err != nil {
		e.poisoned = err
		return err
	}
	cmd, err := recordToCommand(rec)
	if err != nil {
		return err
	}
	if err := e.sm.Apply(cmd); err != nil {
		return err
	}
	e.lastIndex = rec.Index
	e.appliedIndex = rec.Index
	return nil
}

func (e *Engine) LastIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastIndex
}

func (e *Engine) AppliedIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.appliedIndex
}

func (e *Engine) errIfNotWritable() error {
	if e.closed {
		return fmt.Errorf("engine: closed")
	}
	if e.poisoned != nil {
		return fmt.Errorf("engine: poisoned: %w", e.poisoned)
	}
	return nil
}

func (e *Engine) setSyncHook(fn func() error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.wal.SetSyncHook(fn)
}

func (e *Engine) Get(key string) (string, bool) {
	return e.sm.Get(key)
}

func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.closed {
		return nil
	}
	e.closed = true
	if e.wal == nil {
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	return err
}

func recordToCommand(r wal.Record) (store.Command, error) {
	cmd := store.Command{Key: r.Key, Value: r.Value}
	switch r.OpType {
	case wal.OpTypePut:
		cmd.Type = store.CommandTypePut
	case wal.OpTypeDelete:
		cmd.Type = store.CommandTypeDelete
	default:
		return store.Command{}, fmt.Errorf("engine: unsupported wal op: %q", r.OpType)
	}
	return cmd, nil
}
