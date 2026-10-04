package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/deepanker/deepstore/internal/errs"
)

const (
	walFileName = "wal.log"
	tmpFileName = "wal.log.tmp"
)

type WAL struct {
	path      string
	f         *os.File
	syncFn    func() error
	lastIndex uint64
	baseIndex uint64
}

func Open(dir string, apply func(Record) error) (*WAL, error) {
	return open(dir, 0, apply)
}

func OpenAfter(dir string, baseIndex uint64, apply func(Record) error) (*WAL, error) {
	return open(dir, baseIndex, apply)
}

func open(dir string, baseIndex uint64, apply func(Record) error) (*WAL, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("wal: mkdir: %w", err)
	}

	path := filepath.Join(dir, walFileName)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if !os.IsExist(err) {
			return nil, fmt.Errorf("wal: create: %w", err)
		}
		f, err = os.OpenFile(path, os.O_RDWR, 0o644)
		if err != nil {
			return nil, fmt.Errorf("wal: open: %w", err)
		}
	} else {
		if err := syncDir(dir); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	w := &WAL{path: path, f: f, baseIndex: baseIndex}

	if _, err := w.recover(apply); err != nil {
		_ = f.Close()
		return nil, err
	}
	if err := w.DiscardThrough(baseIndex); err != nil {
		_ = w.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) LastIndex() uint64 {
	return w.lastIndex
}

func (w *WAL) Append(r Record) error {
	return w.AppendMany([]Record{r})
}

func (w *WAL) AppendMany(recs []Record) error {
	if len(recs) == 0 {
		return nil
	}
	buf := make([]byte, 0, 64*len(recs))
	for i := range recs {
		want := w.lastIndex + uint64(i) + 1
		if recs[i].Index != want {
			return fmt.Errorf("wal: index %d, want %d", recs[i].Index, want)
		}
		frame, err := recs[i].Encode()
		if err != nil {
			return err
		}
		buf = append(buf, frame...)
	}
	n, err := w.f.Write(buf)
	if n != len(buf) {
		if err != nil {
			return fmt.Errorf("wal: write: %w", err)
		}
		return fmt.Errorf("wal: short write: wrote %d of %d bytes", n, len(buf))
	}
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	w.lastIndex = recs[len(recs)-1].Index
	return nil
}

// SetSyncHook installs a test hook that runs before each fsync; a non-nil error skips the fsync.
// Like AppendMany, it is not safe to call while Sync is running.
func (w *WAL) SetSyncHook(fn func() error) {
	w.syncFn = fn
}

func (w *WAL) Sync() error {
	if w.syncFn != nil {
		if err := w.syncFn(); err != nil {
			return fmt.Errorf("wal: sync: %w", err)
		}
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

func (w *WAL) Replay(apply func(Record) error) error {
	_, err := w.recover(apply)
	return err
}

func (w *WAL) Close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func (w *WAL) DiscardThrough(index uint64) error {
	if w.f == nil {
		return fmt.Errorf("wal: closed")
	}
	if index == 0 {
		return nil
	}

	dir := filepath.Dir(w.path)
	tmp := filepath.Join(dir, tmpFileName)
	if err := os.Remove(tmp); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("wal: remove tmp: %w", err)
	}

	tail, err := w.tailOffset(index)
	if err != nil {
		return err
	}
	if tail == 0 {
		if _, err := w.f.Seek(0, io.SeekEnd); err != nil {
			return fmt.Errorf("wal: seek end: %w", err)
		}
		return nil
	}

	if err := w.writeTail(tmp, tail); err != nil {
		_ = os.Remove(tmp)
		return err
	}

	if err := w.f.Close(); err != nil {
		w.f = nil
		_ = os.Remove(tmp)
		if f, oerr := os.OpenFile(w.path, os.O_RDWR, 0o644); oerr == nil {
			w.f = f
			_, _ = w.f.Seek(0, io.SeekEnd)
			return fmt.Errorf("wal: close: %w", err)
		}
		return fmt.Errorf("%w: close: %v", errs.ErrUnavailable, err)
	}
	w.f = nil

	if err := os.Rename(tmp, w.path); err != nil {
		_ = os.Remove(tmp)
		if f, oerr := os.OpenFile(w.path, os.O_RDWR, 0o644); oerr == nil {
			w.f = f
			_, _ = w.f.Seek(0, io.SeekEnd)
			return fmt.Errorf("wal: rename: %w", err)
		}
		return fmt.Errorf("%w: rename: %v", errs.ErrUnavailable, err)
	}
	if err := syncDir(dir); err != nil {
		if f, oerr := os.OpenFile(w.path, os.O_RDWR, 0o644); oerr != nil {
			return fmt.Errorf("%w: %v", errs.ErrUnavailable, oerr)
		} else {
			w.f = f
			_, _ = f.Seek(0, io.SeekEnd)
		}
		return err
	}

	f, err := os.OpenFile(w.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("%w: %w", errs.ErrUnavailable, err)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		_ = f.Close()
		return fmt.Errorf("%w: %w", errs.ErrUnavailable, err)
	}
	w.f = f
	return nil
}

func (w *WAL) tailOffset(index uint64) (int64, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("wal: seek: %w", err)
	}

	var tail int64 = -1
	header := make([]byte, 8)
	for {
		offset, err := w.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return 0, fmt.Errorf("wal: seek: %w", err)
		}
		_, err = io.ReadFull(w.f, header)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("wal: read header: %w", err)
		}
		payloadLen := binary.LittleEndian.Uint32(header[4:8])
		if payloadLen > maxPayloadSize {
			return 0, fmt.Errorf("wal: corrupt record at offset %d: payload length %d exceeds max %d", offset, payloadLen, maxPayloadSize)
		}
		payload := make([]byte, payloadLen)
		if _, err := io.ReadFull(w.f, payload); err != nil {
			return 0, fmt.Errorf("wal: read payload: %w", err)
		}
		frame := make([]byte, 8+len(payload))
		copy(frame[:8], header)
		copy(frame[8:], payload)
		var rec Record
		if err := rec.Decode(frame); err != nil {
			return 0, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
		}
		if rec.Index > index {
			if tail < 0 {
				if offset == 0 {
					return 0, nil
				}
				tail = offset
			}
		}
	}
	if tail < 0 {
		end, err := w.f.Seek(0, io.SeekEnd)
		if err != nil {
			return 0, fmt.Errorf("wal: seek end: %w", err)
		}
		return end, nil
	}
	return tail, nil
}

func (w *WAL) writeTail(tmp string, tail int64) error {
	tf, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("wal: create tmp: %w", err)
	}
	if _, err := w.f.Seek(tail, io.SeekStart); err != nil {
		_ = tf.Close()
		return fmt.Errorf("wal: seek: %w", err)
	}
	if _, err := io.Copy(tf, w.f); err != nil {
		_ = tf.Close()
		return fmt.Errorf("wal: copy tail: %w", err)
	}
	if err := tf.Sync(); err != nil {
		_ = tf.Close()
		return fmt.Errorf("wal: sync tmp: %w", err)
	}
	if err := tf.Close(); err != nil {
		return fmt.Errorf("wal: close tmp: %w", err)
	}
	return nil
}

func (w *WAL) recover(apply func(Record) error) (int64, error) {
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return 0, fmt.Errorf("wal: seek: %w", err)
	}

	var lastGood int64
	var lastIndex uint64
	header := make([]byte, 8)

	for {
		offset, err := w.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return lastGood, fmt.Errorf("wal: seek: %w", err)
		}

		_, err = io.ReadFull(w.f, header)
		if err == io.EOF {
			break
		}
		if err == io.ErrUnexpectedEOF {
			if err := w.truncate(lastGood); err != nil {
				return lastGood, err
			}
			break
		}
		if err != nil {
			return lastGood, fmt.Errorf("wal: read header: %w", err)
		}

		payloadLen := binary.LittleEndian.Uint32(header[4:8])
		if payloadLen > maxPayloadSize {
			return lastGood, fmt.Errorf("wal: corrupt record at offset %d: payload length %d exceeds max %d", offset, payloadLen, maxPayloadSize)
		}
		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(w.f, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if err := w.truncate(lastGood); err != nil {
				return lastGood, err
			}
			break
		}
		if err != nil {
			return lastGood, fmt.Errorf("wal: read payload: %w", err)
		}

		frame := make([]byte, 8+len(payload))
		copy(frame[:8], header)
		copy(frame[8:], payload)

		var rec Record
		if err := rec.Decode(frame); err != nil {
			if errors.Is(err, errCRCMismatch) {
				follows, ferr := w.validRecordFollows(offset + int64(len(frame)))
				if ferr != nil {
					return lastGood, ferr
				}
				if !follows {
					if err := w.truncate(lastGood); err != nil {
						return lastGood, err
					}
					break
				}
			}
			return lastGood, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
		}
		if lastIndex == 0 {
			if err := checkFirstIndex(rec.Index, w.baseIndex); err != nil {
				return lastGood, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
			}
		} else if rec.Index != lastIndex+1 {
			return lastGood, fmt.Errorf("wal: corrupt record at offset %d: index %d, want %d", offset, rec.Index, lastIndex+1)
		}

		if apply != nil && rec.Index > w.baseIndex {
			if err := apply(rec); err != nil {
				return lastGood, err
			}
		}

		lastIndex = rec.Index
		lastGood = offset + int64(len(frame))
	}

	if lastIndex < w.baseIndex {
		if err := w.truncate(0); err != nil {
			return lastGood, err
		}
		w.lastIndex = w.baseIndex
		return 0, nil
	}
	if err := w.truncate(lastGood); err != nil {
		return lastGood, err
	}
	if _, err := w.f.Seek(lastGood, io.SeekStart); err != nil {
		return lastGood, fmt.Errorf("wal: seek end: %w", err)
	}
	w.lastIndex = lastIndex
	return lastGood, nil
}

func checkFirstIndex(index, base uint64) error {
	if index == 1 {
		return nil
	}
	if base > 0 && index <= base+1 {
		return nil
	}
	if base == 0 {
		return fmt.Errorf("index %d, want 1", index)
	}
	return fmt.Errorf("index %d, want 1 or at most %d", index, base+1)
}

func (w *WAL) validRecordFollows(start int64) (bool, error) {
	end, err := w.f.Seek(0, io.SeekEnd)
	if err != nil {
		return false, fmt.Errorf("wal: seek: %w", err)
	}

	offset := start
	for offset < end {
		if _, err := w.f.Seek(offset, io.SeekStart); err != nil {
			return false, fmt.Errorf("wal: seek: %w", err)
		}

		header := make([]byte, 8)
		_, err := io.ReadFull(w.f, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("wal: read header: %w", err)
		}

		payloadLen := binary.LittleEndian.Uint32(header[4:8])
		if payloadLen > maxPayloadSize {
			return false, fmt.Errorf("wal: corrupt record at offset %d: payload length %d exceeds max %d", offset, payloadLen, maxPayloadSize)
		}
		payload := make([]byte, payloadLen)
		_, err = io.ReadFull(w.f, payload)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("wal: read payload: %w", err)
		}

		frame := make([]byte, 8+len(payload))
		copy(frame[:8], header)
		copy(frame[8:], payload)

		var rec Record
		if err := rec.Decode(frame); err != nil {
			if errors.Is(err, errCRCMismatch) {
				offset += int64(len(frame))
				continue
			}
			return false, fmt.Errorf("wal: corrupt record at offset %d: %w", offset, err)
		}
		return true, nil
	}
	return false, nil
}

func (w *WAL) truncate(size int64) error {
	if err := w.f.Truncate(size); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync after truncate: %w", err)
	}
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: sync dir: %w", err)
	}
	return nil
}
