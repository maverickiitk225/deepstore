package engine

import (
	"errors"
	"fmt"
	"sync"

	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/snapshot"
	"github.com/deepanker/deepstore/internal/store"
	"github.com/deepanker/deepstore/internal/wal"
)

// DefaultSnapshotEvery is the number of applied records between snapshots.
const DefaultSnapshotEvery uint64 = 1024

type Engine struct {
	dir           string
	wal           *wal.WAL
	sm            *store.StateMachine
	mu            sync.Mutex
	cond          *sync.Cond
	queue         []commitReq
	snapWait      []chan error
	done          chan struct{}
	holdBatch     bool // tests set this so the writer waits until the queue is full
	closed        bool
	poisoned      error
	lastIndex     uint64
	appliedIndex  uint64
	snapshotEvery uint64
	snapshotIndex uint64
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
	return open(dir, DefaultSnapshotEvery)
}

func open(dir string, every uint64) (*Engine, error) {
	sm := store.NewStateMachine()
	snap, ok, err := snapshot.Load(dir)
	if err != nil {
		return nil, err
	}
	var base uint64
	if ok {
		base = snap.Index
		sm.Restore(snap)
	}
	w, err := wal.OpenAfter(dir, base, func(r wal.Record) error {
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
		dir:           dir,
		wal:           w,
		sm:            sm,
		done:          make(chan struct{}),
		lastIndex:     w.LastIndex(),
		appliedIndex:  w.LastIndex(),
		snapshotEvery: every,
		snapshotIndex: base,
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

func (e *Engine) Snapshot() error {
	resp := make(chan error, 1)
	e.mu.Lock()
	if err := e.errIfNotWritable(); err != nil {
		e.mu.Unlock()
		return err
	}
	e.snapWait = append(e.snapWait, resp)
	e.cond.Signal()
	e.mu.Unlock()
	return <-resp
}

func (e *Engine) SetSnapshotEvery(n uint64) {
	e.mu.Lock()
	e.snapshotEvery = n
	e.mu.Unlock()
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
		for len(e.queue) == 0 && len(e.snapWait) == 0 && !e.closed {
			e.cond.Wait()
		}
		if len(e.queue) > 0 {
			batch := e.queue
			e.queue = nil
			e.mu.Unlock()
			e.commitBatch(batch)
			e.maybeSnapshot()
			e.mu.Lock()
			continue
		}
		if len(e.snapWait) > 0 && !e.closed {
			waiters := e.snapWait
			e.snapWait = nil
			e.mu.Unlock()
			err := e.installSnapshot()
			for _, ch := range waiters {
				ch <- err
			}
			e.mu.Lock()
			continue
		}
		for _, ch := range e.snapWait {
			ch <- fmt.Errorf("engine: closed")
		}
		e.snapWait = nil
		e.mu.Unlock()
		return
	}
}

func (e *Engine) maybeSnapshot() {
	e.mu.Lock()
	every := e.snapshotEvery
	due := every > 0 && e.poisoned == nil && e.appliedIndex > e.snapshotIndex && e.appliedIndex-e.snapshotIndex >= every
	e.mu.Unlock()
	if !due {
		return
	}
	_ = e.installSnapshot()
}

func (e *Engine) installSnapshot() error {
	e.mu.Lock()
	if e.poisoned != nil {
		err := fmt.Errorf("engine: poisoned: %w", e.poisoned)
		e.mu.Unlock()
		return err
	}
	if e.wal == nil {
		e.mu.Unlock()
		return fmt.Errorf("engine: closed")
	}
	index := e.appliedIndex
	if index == 0 {
		e.mu.Unlock()
		return nil
	}
	needSave := index > e.snapshotIndex
	dir := e.dir
	snapAt := e.snapshotIndex
	e.mu.Unlock()

	if needSave {
		data, sessions := e.sm.Export()
		snapSessions := make(map[uint64]snapshot.Session, len(sessions))
		for id, sess := range sessions {
			snapSessions[id] = snapshot.Session{Seq: sess.Seq, Index: sess.Index, Swapped: sess.Swapped}
		}
		if err := snapshot.Save(dir, snapshot.Snapshot{Index: index, Data: data, Sessions: snapSessions}); err != nil {
			return fmt.Errorf("engine: snapshot: %w", err)
		}
		e.mu.Lock()
		if index > e.snapshotIndex {
			e.snapshotIndex = index
		}
		e.mu.Unlock()
		snapAt = index
	}
	return e.compactLog(snapAt)
}

func (e *Engine) compactLog(index uint64) error {
	if err := e.wal.DiscardThrough(index); err != nil {
		e.mu.Lock()
		if errors.Is(err, errs.ErrUnavailable) {
			e.poisoned = err
		}
		e.mu.Unlock()
		return fmt.Errorf("engine: snapshot: %w", err)
	}
	return nil
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

func (e *Engine) SnapshotIndex() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.snapshotIndex
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
