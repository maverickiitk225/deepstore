package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	base string
	hc   *http.Client
}

func New(addr string) *Client {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return &Client{
		base: strings.TrimRight(addr, "/"),
		hc:   &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) Put(ctx context.Context, key, value string) (uint64, error) {
	body, err := json.Marshal(struct {
		Value string `json:"value"`
	}{Value: value})
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

func (c *Client) Delete(ctx context.Context, key string) (uint64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.keyURL(key), nil)
	if err != nil {
		return 0, err
	}
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
