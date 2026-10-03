package engine

import (
	"fmt"
	"sync"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/deepanker/deepstore/internal/wal"
)

type Engine struct {
	wal          *wal.WAL
	sm           *store.StateMachine
	mu           sync.Mutex
	cond         *sync.Cond
	queue        []commitReq
	done         chan struct{}
	holdBatch    bool // tests set this so the writer waits until the queue is full
	closed       bool
	poisoned     error
	lastIndex    uint64
	appliedIndex uint64
}

type commitReq struct {
	rec  wal.Record
	cmd  store.Command
	resp chan commitResp
}

type commitResp struct {
	index   uint64
	swapped bool
	err     error
}

func Open(dir string) (*Engine, error) {
	sm := store.NewStateMachine()
	w, err := wal.Open(dir, func(r wal.Record) error {
		cmd, err := recordToCommand(r)
		if err != nil {
			return err
		}
		_, err = sm.Apply(cmd)
		return err
	})
	if err != nil {
		return nil, err
	}
	e := &Engine{
		wal:          w,
		sm:           sm,
		done:         make(chan struct{}),
		lastIndex:    w.LastIndex(),
		appliedIndex: w.LastIndex(),
	}
	e.cond = sync.NewCond(&e.mu)
	go e.writer()
	return e, nil
}

func (e *Engine) Put(key, value string, clientID, seq uint64) (uint64, error) {
	if key == "" {
		return 0, fmt.Errorf("engine: empty key")
	}
	if err := requireSession(clientID, seq); err != nil {
		return 0, err
	}
	index, _, err := e.commit(wal.Record{OpType: wal.OpTypePut, Key: key, Value: value, ClientID: clientID, Seq: seq})
	return index, err
}

func (e *Engine) Delete(key string, clientID, seq uint64) (uint64, error) {
	if key == "" {
		return 0, fmt.Errorf("engine: empty key")
	}
	if err := requireSession(clientID, seq); err != nil {
		return 0, err
	}
	index, _, err := e.commit(wal.Record{OpType: wal.OpTypeDelete, Key: key, ClientID: clientID, Seq: seq})
	return index, err
}

// CAS swaps key to value when the key is present and its current value equals
// expected. A missing key does not match. A new sequence is appended either way,
// so a false result still advances the log index and replays as a no-op.
// A repeated sequence returns the original result and is not appended.
func (e *Engine) CAS(key, expected, value string, clientID, seq uint64) (uint64, bool, error) {
	if key == "" {
		return 0, false, fmt.Errorf("engine: empty key")
	}
	if err := requireSession(clientID, seq); err != nil {
		return 0, false, err
	}
	return e.commit(wal.Record{
		OpType:   wal.OpTypeCAS,
		Key:      key,
		Value:    value,
		Expected: expected,
		ClientID: clientID,
		Seq:      seq,
	})
}

func requireSession(clientID, seq uint64) error {
	if clientID == 0 || seq == 0 {
		return fmt.Errorf("engine: %w: client id and seq are required", errs.ErrSession)
	}
	return nil
}

func (e *Engine) commit(rec wal.Record) (uint64, bool, error) {
	cmd, err := recordToCommand(rec)
	if err != nil {
		return 0, false, err
	}
	resp := make(chan commitResp, 1)
	e.mu.Lock()
	if err := e.errIfNotWritable(); err != nil {
		e.mu.Unlock()
		return 0, false, err
	}
	e.queue = append(e.queue, commitReq{rec: rec, cmd: cmd, resp: resp})
	e.cond.Signal()
	e.mu.Unlock()

	r := <-resp
	return r.index, r.swapped, r.err
}

func (e *Engine) writer() {
	defer close(e.done)
	e.mu.Lock()
	for {
		for e.holdBatch && !e.closed {
			e.cond.Wait()
		}
		for len(e.queue) == 0 && !e.closed {
			e.cond.Wait()
		}
		if len(e.queue) == 0 {
			e.mu.Unlock()
			return
		}
		batch := e.queue
		e.queue = nil
		e.mu.Unlock()
		e.commitBatch(batch)
		e.mu.Lock()
	}
}

func (e *Engine) commitBatch(batch []commitReq) {
	e.mu.Lock()
	if e.poisoned != nil {
		err := fmt.Errorf("engine: poisoned: %w", e.poisoned)
		e.mu.Unlock()
		replyError(batch, err)
		return
	}

	initial := e.sm.Sessions()
	spec := make(map[uint64]uint64, len(initial))
	for id, sess := range initial {
		spec[id] = sess.Seq
	}
	var durable, dups, bad []int
	badErr := make(map[int]error)
	for i := range batch {
		id := batch[i].rec.ClientID
		seq := batch[i].rec.Seq
		prev := spec[id]
		switch {
		case seq == prev+1:
			durable = append(durable, i)
			spec[id] = seq
		case prev > 0 && seq == prev:
			dups = append(dups, i)
		default:
			bad = append(bad, i)
			badErr[i] = fmt.Errorf("engine: %w: client %d seq %d, want %d", errs.ErrSession, id, seq, prev+1)
		}
	}

	if len(durable) == 0 {
		e.mu.Unlock()
		replySession(batch, dups, bad, badErr, initial)
		return
	}

	recs := make([]wal.Record, len(durable))
	for n, i := range durable {
		batch[i].rec.Index = e.lastIndex + uint64(n) + 1
		batch[i].cmd.Index = batch[i].rec.Index
		recs[n] = batch[i].rec
	}
	if err := e.wal.AppendMany(recs); err != nil {
		e.poisoned = err
		e.mu.Unlock()
		replyError(batch, err)
		return
	}
	e.mu.Unlock()

	if err := e.wal.Sync(); err != nil {
		e.mu.Lock()
		e.poisoned = err
		e.mu.Unlock()
		replyError(batch, err)
		return
	}

	applied := make([]commitResp, len(batch))
	e.mu.Lock()
	for _, i := range durable {
		res, err := e.sm.Apply(batch[i].cmd)
		if err != nil {
			panic(fmt.Sprintf("engine: apply durable record %d: %v", batch[i].rec.Index, err))
		}
		applied[i] = commitResp{index: res.Index, swapped: res.Swapped}
		e.lastIndex = batch[i].rec.Index
		e.appliedIndex = batch[i].rec.Index
	}
	answered := e.sm.Sessions()
	e.mu.Unlock()

	for _, i := range durable {
		batch[i].resp <- applied[i]
	}
	for _, i := range dups {
		sess := answered[batch[i].rec.ClientID]
		batch[i].resp <- commitResp{index: sess.Index, swapped: sess.Swapped}
	}
	for _, i := range bad {
		batch[i].resp <- commitResp{err: badErr[i]}
	}
}

func replySession(batch []commitReq, dups, bad []int, badErr map[int]error, sessions map[uint64]store.Session) {
	for _, i := range bad {
		batch[i].resp <- commitResp{err: badErr[i]}
	}
	for _, i := range dups {
		sess := sessions[batch[i].rec.ClientID]
		batch[i].resp <- commitResp{index: sess.Index, swapped: sess.Swapped}
	}
}

func replyError(batch []commitReq, err error) {
	for i := range batch {
		batch[i].resp <- commitResp{err: err}
	}
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
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	e.holdBatch = false
	e.cond.Broadcast()
	e.mu.Unlock()

	<-e.done

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.wal == nil {
		return nil
	}
	err := e.wal.Close()
	e.wal = nil
	return err
}

func recordToCommand(r wal.Record) (store.Command, error) {
	cmd := store.Command{
		Key:      r.Key,
		Value:    r.Value,
		Expected: r.Expected,
		ClientID: r.ClientID,
		Seq:      r.Seq,
		Index:    r.Index,
	}
	switch r.OpType {
	case wal.OpTypePut:
		cmd.Type = store.CommandTypePut
	case wal.OpTypeDelete:
		cmd.Type = store.CommandTypeDelete
	case wal.OpTypeCAS:
		cmd.Type = store.CommandTypeCAS
	default:
		return store.Command{}, fmt.Errorf("engine: unsupported wal op: %q", r.OpType)
	}
	return cmd, nil
}
