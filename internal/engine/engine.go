package engine

import (
	"fmt"
	"sync"

	"github.com/deepanker/deepstore/internal/store"
	"github.com/deepanker/deepstore/internal/wal"
)

type Engine struct {
	wal *wal.WAL
	sm  *store.StateMachine
	mu  sync.Mutex
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
	return &Engine{wal: w, sm: sm}, nil
}

func (e *Engine) Put(key, value string) error {
	if key == "" {
		return fmt.Errorf("engine: empty key")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	rec := wal.Record{OpType: wal.OpTypePut, Key: key, Value: value}
	if err := e.wal.Append(rec); err != nil {
		return err
	}
	if err := e.wal.Sync(); err != nil {
		return err
	}
	cmd, err := recordToCommand(rec)
	if err != nil {
		return err
	}
	return e.sm.Apply(cmd)
}

func (e *Engine) Delete(key string) error {
	if key == "" {
		return fmt.Errorf("engine: empty key")
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	rec := wal.Record{OpType: wal.OpTypeDelete, Key: key}
	if err := e.wal.Append(rec); err != nil {
		return err
	}
	if err := e.wal.Sync(); err != nil {
		return err
	}
	cmd, err := recordToCommand(rec)
	if err != nil {
		return err
	}
	return e.sm.Apply(cmd)
}

func (e *Engine) Get(key string) (string, bool) {
	return e.sm.Get(key)
}

func (e *Engine) Close() error {
	if e.wal == nil {
		return nil
	}
	return e.wal.Close()
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
