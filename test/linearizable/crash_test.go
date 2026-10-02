package linearizable

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/deepanker/deepstore/internal/client"
)

func TestModelUnknownPutMayExistOrNot(t *testing.T) {
	put := porcupine.Operation{
		ClientId: 0,
		Input:    kvInput{Op: "put", Key: "k", Value: "v"},
		Call:     0,
		Output:   kvOutput{Unknown: true},
		Return:   10,
	}
	sawValue := porcupine.Operation{
		ClientId: 1,
		Input:    kvInput{Op: "get", Key: "k"},
		Call:     20,
		Output:   kvOutput{Value: "v", Present: true},
		Return:   30,
	}
	sawMissing := porcupine.Operation{
		ClientId: 1,
		Input:    kvInput{Op: "get", Key: "k"},
		Call:     20,
		Output:   kvOutput{},
		Return:   30,
	}
	if res := check([]porcupine.Operation{put, sawValue}); res != porcupine.Ok {
		t.Fatalf("unknown put then get value: %s, want Ok", res)
	}
	if res := check([]porcupine.Operation{put, sawMissing}); res != porcupine.Ok {
		t.Fatalf("unknown put then get missing: %s, want Ok", res)
	}
}

func TestServerPutGetLinearizableUnderCrash(t *testing.T) {
	bin := buildServer(t)
	dir := t.TempDir()
	addr := freeAddr(t)

	var proc serverProc
	t.Cleanup(proc.kill)
	if err := proc.start(bin, dir, addr); err != nil {
		t.Fatal(err)
	}

	const clients = 8
	keys := []string{"a", "b", "c", "d", "e"}
	base := "http://" + addr
	stop := make(chan struct{})

	var mu sync.Mutex
	var history []porcupine.Operation
	var unknown atomic.Int64
	errCh := make(chan error, clients)

	var wg sync.WaitGroup
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := client.New(base)
			rng := rand.New(rand.NewSource(int64(id + 1)))
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := keys[rng.Intn(len(keys))]
				if rng.Intn(3) != 0 {
					value := fmt.Sprintf("%d-%d", id, n)
					call := time.Now().UnixNano()
					ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
					_, err := c.Put(ctx, key, value)
					cancel()
					ret := time.Now().UnixNano()
					if err != nil {
						if isConnRefused(err) {
							continue
						}
						if isMaybeApplied(err) {
							unknown.Add(1)
							record(&mu, &history, porcupine.Operation{
								ClientId: id,
								Input:    kvInput{Op: "put", Key: key, Value: value},
								Call:     call,
								Output:   kvOutput{Unknown: true},
								Return:   ret,
							})
							continue
						}
						errCh <- fmt.Errorf("client %d put %s: %w", id, key, err)
						return
					}
					record(&mu, &history, porcupine.Operation{
						ClientId: id,
						Input:    kvInput{Op: "put", Key: key, Value: value},
						Call:     call,
						Output:   kvOutput{},
						Return:   ret,
					})
					continue
				}
				call := time.Now().UnixNano()
				ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
				value, ok, err := c.Get(ctx, key)
				cancel()
				ret := time.Now().UnixNano()
				if err != nil {
					if isConnRefused(err) || isMaybeApplied(err) {
						continue
					}
					errCh <- fmt.Errorf("client %d get %s: %w", id, key, err)
					return
				}
				record(&mu, &history, porcupine.Operation{
					ClientId: id,
					Input:    kvInput{Op: "get", Key: key},
					Call:     call,
					Output:   kvOutput{Value: value, Present: ok},
					Return:   ret,
				})
			}
		}(id)
	}

	for i := 0; i < 3; i++ {
		time.Sleep(500 * time.Millisecond)
		proc.kill()
		time.Sleep(80 * time.Millisecond)
		if err := proc.start(bin, dir, addr); err != nil {
			close(stop)
			wg.Wait()
			t.Fatal(err)
		}
	}
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}
	if unknown.Load() == 0 {
		t.Fatal("crash injection produced no uncertain puts")
	}

	res, info := porcupine.CheckOperationsVerbose(kvModel(), history, 30*time.Second)
	if res == porcupine.Ok {
		return
	}
	f, err := os.CreateTemp("", "deepstore-crash-history-*.html")
	if err != nil {
		t.Fatal(err)
	}
	if err := porcupine.Visualize(kvModel(), info, f); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	t.Fatalf("history is %s; visualization: %s", res, f.Name())
}

func buildServer(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "deepstore")
	cmd := exec.Command("go", "build", "-o", bin, "./cmd/deepstore")
	cmd.Dir = filepath.Join("..", "..")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("build server: %v\n%s", err, out)
	}
	return bin
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

type serverProc struct {
	mu  sync.Mutex
	cmd *exec.Cmd
}

func (p *serverProc) start(bin, dir, addr string) error {
	p.kill()
	var last error
	for i := 0; i < 50; i++ {
		cmd := exec.Command(bin, "serve", "-data-dir", dir, "-listen", addr)
		cmd.Stdout = io.Discard
		cmd.Stderr = io.Discard
		if err := cmd.Start(); err != nil {
			last = err
			time.Sleep(20 * time.Millisecond)
			continue
		}
		p.mu.Lock()
		p.cmd = cmd
		p.mu.Unlock()
		if waitReady(addr) {
			return nil
		}
		p.kill()
		last = fmt.Errorf("server did not become ready")
		time.Sleep(30 * time.Millisecond)
	}
	return last
}

func (p *serverProc) kill() {
	p.mu.Lock()
	cmd := p.cmd
	p.cmd = nil
	p.mu.Unlock()
	if cmd == nil || cmd.Process == nil {
		return
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
}

func waitReady(addr string) bool {
	c := client.New("http://" + addr)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		_, _, err := c.Get(ctx, "ready")
		cancel()
		if err == nil {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

func isConnRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}

func isMaybeApplied(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) {
		return true
	}
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
