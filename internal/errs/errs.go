package errs

import "errors"

// ErrSession is a client sequence that is not the next command and not a retry
// of the latest one. The record is not appended.
var ErrSession = errors.New("session")

// ErrUnavailable means the live log could not be reopened after it was closed.
// The engine poisons; the next open recovers from the snapshot.
var ErrUnavailable = errors.New("wal: log unavailable")
