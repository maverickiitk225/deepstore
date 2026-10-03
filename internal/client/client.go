package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"
)

type Client struct {
	base     string
	hc       *http.Client
	clientID uint64
	seq      atomic.Uint64
}

func New(addr string) *Client {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("client: random id: %v", err))
	}
	id := binary.LittleEndian.Uint64(buf[:])
	if id == 0 {
		id = 1
	}
	return &Client{
		base:     strings.TrimRight(addr, "/"),
		hc:       &http.Client{Timeout: 30 * time.Second},
		clientID: id,
	}
}

// NextSeq returns the next sequence for a new command. A retry of a command
// whose response was lost must reuse the sequence from the first attempt.
func (c *Client) NextSeq() uint64 {
	return c.seq.Add(1)
}

func (c *Client) Put(ctx context.Context, key, value string) (uint64, error) {
	return c.PutSeq(ctx, key, value, c.NextSeq())
}

func (c *Client) PutSeq(ctx context.Context, key, value string, seq uint64) (uint64, error) {
	body, err := json.Marshal(struct {
		Value    string `json:"value"`
		ClientID uint64 `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}{Value: value, ClientID: c.clientID, Seq: seq})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.keyURL(key), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	var out struct {
		Index uint64 `json:"index"`
	}
	if err := c.do(req, &out); err != nil {
		return 0, err
	}
	return out.Index, nil
}

func (c *Client) Get(ctx context.Context, key string) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.keyURL(key), nil)
	if err != nil {
		return "", false, err
	}
	var out struct {
		Value string `json:"value"`
	}
	if err := c.do(req, &out); err != nil {
		if errors.Is(err, errNotFound) {
			return "", false, nil
		}
		return "", false, err
	}
	return out.Value, true, nil
}

func (c *Client) CAS(ctx context.Context, key, expected, value string) (uint64, bool, error) {
	return c.CASSeq(ctx, key, expected, value, c.NextSeq())
}

func (c *Client) CASSeq(ctx context.Context, key, expected, value string, seq uint64) (uint64, bool, error) {
	body, err := json.Marshal(struct {
		Expected string `json:"expected"`
		Value    string `json:"value"`
		ClientID uint64 `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}{Expected: expected, Value: value, ClientID: c.clientID, Seq: seq})
	if err != nil {
		return 0, false, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.keyURL(key)+"/cas", bytes.NewReader(body))
	if err != nil {
		return 0, false, err
	}
	req.Header.Set("Content-Type", "application/json")
	var out struct {
		Index   uint64 `json:"index"`
		Swapped bool   `json:"swapped"`
	}
	if err := c.do(req, &out); err != nil {
		return 0, false, err
	}
	return out.Index, out.Swapped, nil
}

func (c *Client) Delete(ctx context.Context, key string) (uint64, error) {
	return c.DeleteSeq(ctx, key, c.NextSeq())
}

func (c *Client) DeleteSeq(ctx context.Context, key string, seq uint64) (uint64, error) {
	body, err := json.Marshal(struct {
		ClientID uint64 `json:"client_id"`
		Seq      uint64 `json:"seq"`
	}{ClientID: c.clientID, Seq: seq})
	if err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.keyURL(key), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	var out struct {
		Index uint64 `json:"index"`
	}
	if err := c.do(req, &out); err != nil {
		return 0, err
	}
	return out.Index, nil
}

func (c *Client) keyURL(key string) string {
	return c.base + "/v1/keys/" + url.PathEscape(key)
}

func (c *Client) do(req *http.Request, dst any) error {
	resp, err := c.hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1024))
	if err != nil {
		return err
	}
	if resp.StatusCode == http.StatusNotFound {
		return errNotFound
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var body struct {
			Error string `json:"error"`
		}
		if json.Unmarshal(data, &body) == nil && body.Error != "" {
			return fmt.Errorf("server: %s", body.Error)
		}
		return fmt.Errorf("server: status %d", resp.StatusCode)
	}
	if dst == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, dst); err != nil {
		return fmt.Errorf("client: decode response: %w", err)
	}
	return nil
}

var errNotFound = errors.New("not found")
