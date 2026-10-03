package errs

import "errors"

// ErrSession is a client sequence that is not the next command and not a retry
// of the latest one. The record is not appended.
var ErrSession = errors.New("session")
