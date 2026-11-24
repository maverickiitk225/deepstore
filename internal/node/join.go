package node

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func Join(ctx context.Context, leaderURL, id, raftAddr, httpAddr string) error {
	httpAddr, err := normalizeHTTP(httpAddr)
	if err != nil {
		return err
	}
	raftAddr, err = normalizeRaftAddr(raftAddr)
	if err != nil {
		return err
	}
	if id == "" {
		return fmt.Errorf("node: id is required")
	}
	payload, err := json.Marshal(struct {
		ID       string `json:"id"`
		RaftAddr string `json:"raft_addr"`
		HTTPAddr string `json:"http_addr"`
	}{ID: id, RaftAddr: raftAddr, HTTPAddr: httpAddr})
	if err != nil {
		return err
	}
	target := strings.TrimRight(leaderURL, "/")
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	client := &http.Client{Timeout: 20 * time.Second}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	var last error
	for {
		if err := ctx.Err(); err != nil {
			if last != nil {
				return fmt.Errorf("node: join: %w", last)
			}
			return fmt.Errorf("node: join: %w", err)
		}
		hint, retry, err := postJoin(ctx, client, target, payload)
		if err == nil {
			return nil
		}
		last = err
		if !retry {
			return err
		}
		if hint != "" {
			target = strings.TrimRight(hint, "/")
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		}
	}
}

func postJoin(ctx context.Context, client *http.Client, target string, payload []byte) (string, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target+"/v1/join", bytes.NewReader(payload))
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return "", true, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if err != nil {
		return "", true, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return "", false, nil
	}
	var body struct {
		Error  string `json:"error"`
		Leader string `json:"leader"`
	}
	_ = json.Unmarshal(data, &body)
	msg := body.Error
	if msg == "" {
		msg = fmt.Sprintf("status %d", resp.StatusCode)
	}
	if resp.StatusCode == http.StatusBadRequest {
		return "", false, fmt.Errorf("node: join: %s", msg)
	}
	return body.Leader, true, fmt.Errorf("node: join: %s", msg)
}
