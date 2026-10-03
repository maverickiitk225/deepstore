package store

import (
	"fmt"
	"sync"
)

type CommandType string

const (
	CommandTypePut    CommandType = "put"
	CommandTypeDelete CommandType = "delete"
	CommandTypeCAS    CommandType = "cas"
)

type Command struct {
	Type     CommandType
	Key      string
	Value    string
	Expected string
}

// Result is what Apply produced. Swapped is set only for a CAS whose current
// value matched Expected.
type Result struct {
	Swapped bool
}

type StateMachine struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		data: make(map[string]string),
	}
}

func (s *StateMachine) Apply(cmd Command) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Type {
	case CommandTypePut:
		s.data[cmd.Key] = cmd.Value
	case CommandTypeDelete:
		delete(s.data, cmd.Key)
	case CommandTypeCAS:
		cur, ok := s.data[cmd.Key]
		if !ok || cur != cmd.Expected {
			return Result{}, nil
		}
		s.data[cmd.Key] = cmd.Value
		return Result{Swapped: true}, nil
	default:
		return Result{}, fmt.Errorf("invalid command type: %s", cmd.Type)
	}

	return Result{}, nil
}

func (s *StateMachine) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}
