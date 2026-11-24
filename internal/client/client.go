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
	"sync"
	"sync/atomic"
	"time"
)

type Client struct {
	mu       sync.Mutex
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

// NextSeq returns the next sequence number for a new command.
// If a response was lost, send the same sequence again instead of calling NextSeq.
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
	return c.baseURL() + "/v1/keys/" + url.PathEscape(key)
}

func (c *Client) baseURL() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.base
}

func (c *Client) setBase(base string) {
	base = strings.TrimRight(base, "/")
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	c.mu.Lock()
	c.base = base
	c.mu.Unlock()
}

// If this server is not the leader, the response includes the leader address.
// The same request is then sent to that address.
func (c *Client) do(req *http.Request, dst any) error {
	var body []byte
	if req.Body != nil {
		var err error
		body, err = io.ReadAll(req.Body)
		_ = req.Body.Close()
		if err != nil {
			return err
		}
	}
	const maxHops = 5
	for hop := 0; ; hop++ {
		attempt := req.Clone(req.Context())
		if body != nil {
			attempt.Body = io.NopCloser(bytes.NewReader(body))
			attempt.ContentLength = int64(len(body))
		}
		resp, err := c.hc.Do(attempt)
		if err != nil {
			return err
		}
		data, err := io.ReadAll(io.LimitReader(resp.Body, 16<<20+1024))
		_ = resp.Body.Close()
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusServiceUnavailable && hop < maxHops {
			var hint struct {
				Leader string `json:"leader"`
			}
			if json.Unmarshal(data, &hint) == nil && hint.Leader != "" && !sameBase(c.baseURL(), hint.Leader) {
				next, err := retarget(attempt.URL.String(), hint.Leader)
				if err == nil {
					u, err := url.Parse(next)
					if err == nil {
						req.URL = u
						c.setBase(hint.Leader)
						continue
					}
				}
			}
		}
		if resp.StatusCode == http.StatusNotFound {
			return errNotFound
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			var errBody struct {
				Error string `json:"error"`
			}
			if json.Unmarshal(data, &errBody) == nil && errBody.Error != "" {
				return fmt.Errorf("server: %s", errBody.Error)
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
}

func sameBase(a, b string) bool {
	return strings.TrimRight(a, "/") == strings.TrimRight(b, "/")
}

func retarget(raw, base string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	if !strings.Contains(base, "://") {
		base = "http://" + base
	}
	parsed, err := url.Parse(base)
	if err != nil || parsed.Host == "" {
		return "", fmt.Errorf("client: leader %q", base)
	}
	u.Scheme = parsed.Scheme
	u.Host = parsed.Host
	return u.String(), nil
}

var errNotFound = errors.New("not found")
