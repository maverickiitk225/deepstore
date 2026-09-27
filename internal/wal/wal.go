package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const walFileName = "wal.log"

type WAL struct {
	path      string
	f         *os.File
	syncFn    func() error
	lastIndex uint64
}

func Open(dir string, apply func(Record) error) (*WAL, error) {
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

	w := &WAL{path: path, f: f}

	if _, err := w.recover(apply); err != nil {
		_ = f.Close()
		return nil, err
	}
	return w, nil
}

func (w *WAL) LastIndex() uint64 {
	return w.lastIndex
}

func (w *WAL) Append(r Record) error {
	if r.Index != w.lastIndex+1 {
		return fmt.Errorf("wal: index %d, want %d", r.Index, w.lastIndex+1)
	}
	frame, err := r.Encode()
	if err != nil {
		return err
	}
	n, err := w.f.Write(frame)
	if err != nil {
		return fmt.Errorf("wal: write: %w", err)
	}
	if n != len(frame) {
		return fmt.Errorf("wal: short write: wrote %d of %d bytes", n, len(frame))
	}
	w.lastIndex = r.Index
	return nil
}

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
		if rec.Index != lastIndex+1 {
			return lastGood, fmt.Errorf("wal: corrupt record at offset %d: index %d, want %d", offset, rec.Index, lastIndex+1)
		}

		if apply != nil {
			if err := apply(rec); err != nil {
				return lastGood, err
			}
		}

		lastIndex = rec.Index
		lastGood = offset + int64(len(frame))
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
