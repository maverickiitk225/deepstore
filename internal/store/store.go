package store

import (
	"fmt"
	"sync"
)

type CommandType string

const (
	CommandTypeSet    CommandType = "set"
	CommandTypeGet    CommandType = "get"
	CommandTypeDelete CommandType = "delete"
	CommandTypeClear  CommandType = "clear"
)

type Command struct {
	Type  CommandType
	Key   string
	Value string
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

func (s *StateMachine) Apply(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Type {
	case CommandTypeSet:
		s.data[cmd.Key] = cmd.Value
	case CommandTypeDelete:
		delete(s.data, cmd.Key)
	case CommandTypeClear:
		s.data = make(map[string]string)
	default:
		return fmt.Errorf("invalid command type: %s", cmd.Type)
	}

	return nil
}

func (s *StateMachine) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}
