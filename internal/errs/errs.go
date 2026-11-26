package errs

import "errors"

// ErrSession means the sequence is neither the next command nor a repeat of the latest one.
// Nothing is written to the log.
var ErrSession = errors.New("session")

// ErrUnavailable means the log file could not be opened again after it was closed.
// The engine stops taking writes. The next open loads the snapshot and replays the log.
var ErrUnavailable = errors.New("wal: log unavailable")

var ErrNotLeader = errors.New("not leader")

// ErrConflict means the server id is already used by a different address.
var ErrConflict = errors.New("conflict")
