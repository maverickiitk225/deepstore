package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"syscall"
	"time"
)

type opKind int

const (
	opPut opKind = iota
	opDelete
	opCAS
)

// writeCall is one Put, Delete, or CAS.
type writeCall struct {
	kind     opKind
	key      string
	value    string
	expected string
	seq      uint64
}

func sameCall(a, b *writeCall) bool {
	if a == nil || b == nil || a.kind != b.kind || a.key != b.key {
		return false
	}
	switch a.kind {
	case opPut, opCAS:
		if a.value != b.value {
			return false
		}
	}
	return a.kind != opCAS || a.expected == b.expected
}

// commit sends the write. A write that may already be stored keeps its sequence.
func (c *Client) commit(ctx context.Context, call *writeCall, dst any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()

	if sameCall(c.pending, call) {
		return c.finish(ctx, c.pending, dst)
	}
	if err := c.finishPending(ctx); err != nil {
		return err
	}
	call.seq = c.acked + 1
	return c.finish(ctx, call, dst)
}

func (c *Client) finishPending(ctx context.Context) error {
	if c.pending == nil {
		return nil
	}
	var ignore struct {
		Index   uint64 `json:"index"`
		Swapped bool   `json:"swapped"`
	}
	if err := c.finish(ctx, c.pending, &ignore); err != nil {
		return fmt.Errorf("client: unresolved write: %w", err)
	}
	return nil
}

// finish retries the write until the server accepts it, rejects it, or ctx ends.
func (c *Client) finish(ctx context.Context, call *writeCall, dst any) error {
	sent, ambiguous, err := c.attempt(ctx, call, dst)
	if err == nil {
		c.acked = call.seq
		c.pending = nil
		return nil
	}
	// Drop the sequence when the server cannot have stored this command.
	if !sent || definiteRejection(err) || (dialError(err) && !ambiguous && c.pending != call) {
		if c.pending == call {
			c.pending = nil
		}
		return err
	}
	c.pending = call
	return err
}

func (c *Client) attempt(ctx context.Context, call *writeCall, dst any) (sent, ambiguous bool, err error) {
	for n := 0; ; n++ {
		if err = ctx.Err(); err != nil {
			return sent, ambiguous, err
		}
		var req *http.Request
		req, err = c.request(ctx, call)
		if err != nil {
			return sent, ambiguous, err
		}
		err = c.do(req, dst)
		sent = true
		if err == nil || !uncertain(err) {
			return sent, ambiguous, err
		}
		ambiguous = true
		if err = sleep(ctx, backoff(n)); err != nil {
			return sent, ambiguous, err
		}
	}
}

func (c *Client) request(ctx context.Context, call *writeCall) (*http.Request, error) {
	var payload any
	switch call.kind {
	case opDelete:
		payload = struct {
			ClientID uint64 `json:"client_id"`
			Seq      uint64 `json:"seq"`
		}{ClientID: c.clientID, Seq: call.seq}
	case opCAS:
		payload = struct {
			Expected []byte `json:"expected"`
			Value    []byte `json:"value"`
			ClientID uint64 `json:"client_id"`
			Seq      uint64 `json:"seq"`
		}{Expected: []byte(call.expected), Value: []byte(call.value), ClientID: c.clientID, Seq: call.seq}
	default:
		payload = struct {
			Value    []byte `json:"value"`
			ClientID uint64 `json:"client_id"`
			Seq      uint64 `json:"seq"`
		}{Value: []byte(call.value), ClientID: c.clientID, Seq: call.seq}
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	method := http.MethodPut
	path := "/v1/keys/" + url.PathEscape(call.key)
	switch call.kind {
	case opDelete:
		method = http.MethodDelete
	case opCAS:
		method = http.MethodPost
		path += "/cas"
	}
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL()+path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func backoff(n int) time.Duration {
	if n > 5 {
		n = 5
	}
	base := 5 * time.Millisecond << n
	return base/2 + time.Duration(rand.Int64N(int64(base/2)+1))
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type statusError struct {
	code int
	msg  string
}

func (e *statusError) Error() string {
	if e.msg != "" {
		return fmt.Sprintf("server: %s", e.msg)
	}
	return fmt.Sprintf("server: status %d", e.code)
}

// definiteRejection reports that the server did not store the command.
func definiteRejection(err error) bool {
	var status *statusError
	if errors.As(err, &status) {
		return !uncertainStatus(status.code, status.msg)
	}
	return errors.Is(err, errNotFound)
}

// uncertain reports that the command may already be stored.
func uncertain(err error) bool {
	if err == nil {
		return false
	}
	var status *statusError
	if errors.As(err, &status) {
		return uncertainStatus(status.code, status.msg)
	}
	if errors.Is(err, errNotFound) || dialError(err) {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	return strings.HasPrefix(err.Error(), "client: decode")
}

func uncertainStatus(code int, msg string) bool {
	switch code {
	case http.StatusInternalServerError:
		return !rejectedBeforeLog(msg)
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return code >= 500
	}
}

// rejectedBeforeLog matches errors sent before the command is logged.
func rejectedBeforeLog(msg string) bool {
	return strings.Contains(msg, "command too large") ||
		strings.Contains(msg, "empty key") ||
		strings.Contains(msg, "invalid command") ||
		strings.Contains(msg, "expected set")
}

func dialError(err error) bool {
	if errors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var op *net.OpError
	if errors.As(err, &op) && op.Op == "dial" {
		return true
	}
	var dns *net.DNSError
	return errors.As(err, &dns)
}
