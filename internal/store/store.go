package store

import (
	"fmt"
	"maps"
	"sync"

	"github.com/deepanker/deepstore/internal/snapshot"
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
	ClientID uint64
	Seq      uint64
	Index    uint64
}

// Result is what Apply produced. Index is the log index of the command that
// first ran this client sequence. Swapped is set only for a CAS whose current
// value matched Expected.
type Result struct {
	Index   uint64
	Swapped bool
}

// Session is the latest command applied for one client.
type Session struct {
	Seq     uint64
	Index   uint64
	Swapped bool
}

type StateMachine struct {
	mu       sync.RWMutex
	data     map[string]string
	sessions map[uint64]Session
}

func NewStateMachine() *StateMachine {
	return &StateMachine{
		data:     make(map[string]string),
		sessions: make(map[uint64]Session),
	}
}

func (s *StateMachine) Apply(cmd Command) (Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if cmd.ClientID != 0 {
		if prev, ok := s.sessions[cmd.ClientID]; ok && cmd.Seq == prev.Seq {
			return Result{Index: prev.Index, Swapped: prev.Swapped}, nil
		}
		if err := checkSeq(cmd, s.sessions[cmd.ClientID]); err != nil {
			return Result{}, err
		}
	}

	res := Result{Index: cmd.Index}
	switch cmd.Type {
	case CommandTypePut:
		s.data[cmd.Key] = cmd.Value
	case CommandTypeDelete:
		delete(s.data, cmd.Key)
	case CommandTypeCAS:
		cur, ok := s.data[cmd.Key]
		if ok && cur == cmd.Expected {
			s.data[cmd.Key] = cmd.Value
			res.Swapped = true
		}
	default:
		return Result{}, fmt.Errorf("invalid command type: %s", cmd.Type)
	}
	if cmd.ClientID != 0 {
		s.sessions[cmd.ClientID] = Session{Seq: cmd.Seq, Index: cmd.Index, Swapped: res.Swapped}
	}
	return res, nil
}

func checkSeq(cmd Command, prev Session) error {
	var want uint64 = 1
	if prev.Seq != 0 {
		want = prev.Seq + 1
	}
	if cmd.Seq != want {
		return fmt.Errorf("client %d seq %d, want %d", cmd.ClientID, cmd.Seq, want)
	}
	return nil
}

func (s *StateMachine) Sessions() map[uint64]Session {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.sessions)
}

func (s *StateMachine) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *StateMachine) Export() (map[string]string, map[uint64]Session) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return maps.Clone(s.data), maps.Clone(s.sessions)
}

func (s *StateMachine) Restore(snap snapshot.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data = maps.Clone(snap.Data)
	s.sessions = make(map[uint64]Session, len(snap.Sessions))
	for id, sess := range snap.Sessions {
		s.sessions[id] = Session(sess)
	}
}
