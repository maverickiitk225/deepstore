package linearizable

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/deepanker/deepstore/internal/client"
	"github.com/deepanker/deepstore/internal/engine"
	"github.com/deepanker/deepstore/internal/server"
)

func TestModelAcceptsPutThenGet(t *testing.T) {
	history := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    kvInput{Op: "put", Key: "k", Value: "v"},
			Call:     0,
			Output:   kvOutput{},
			Return:   10,
		},
		{
			ClientId: 1,
			Input:    kvInput{Op: "get", Key: "k"},
			Call:     20,
			Output:   kvOutput{Value: "v", Present: true},
			Return:   30,
		},
	}
	if res := check(history); res != porcupine.Ok {
		t.Fatalf("put then get: %s, want Ok", res)
	}
}

func TestModelRejectsGetMissingAfterPut(t *testing.T) {
	history := []porcupine.Operation{
		{
			ClientId: 0,
			Input:    kvInput{Op: "put", Key: "k", Value: "v"},
			Call:     0,
			Output:   kvOutput{},
			Return:   10,
		},
		{
			ClientId: 1,
			Input:    kvInput{Op: "get", Key: "k"},
			Call:     20,
			Output:   kvOutput{},
			Return:   30,
		},
	}
	if res := check(history); res != porcupine.Illegal {
		t.Fatalf("get missing after put: %s, want Illegal", res)
	}
}

func TestServerPutGetLinearizable(t *testing.T) {
	dir := t.TempDir()
	eng, err := engine.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(eng, ln.Addr().String())
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srv.Serve(ln)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
		_ = eng.Close()
		err := <-serveErr
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			t.Errorf("server: %v", err)
		}
	})

	const (
		clients = 8
		ops     = 20
	)
	keys := []string{"a", "b", "c", "d", "e"}
	addr := "http://" + ln.Addr().String()

	var mu sync.Mutex
	history := make([]porcupine.Operation, 0, clients*ops)
	errCh := make(chan error, clients)

	var wg sync.WaitGroup
	for id := 0; id < clients; id++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			c := client.New(addr)
			rng := rand.New(rand.NewSource(int64(id + 1)))
			for n := 0; n < ops; n++ {
				key := keys[rng.Intn(len(keys))]
				if rng.Intn(2) == 0 {
					value := fmt.Sprintf("%d-%d", id, n)
					call := time.Now().UnixNano()
					_, err := c.Put(context.Background(), key, value)
					ret := time.Now().UnixNano()
					if err != nil {
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
				value, ok, err := c.Get(context.Background(), key)
				ret := time.Now().UnixNano()
				if err != nil {
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
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if t.Failed() {
		return
	}

	res, info := porcupine.CheckOperationsVerbose(kvModel(), history, 30*time.Second)
	if res == porcupine.Ok {
		return
	}
	f, err := os.CreateTemp("", "deepstore-history-*.html")
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

func check(history []porcupine.Operation) porcupine.CheckResult {
	res, _ := porcupine.CheckOperationsVerbose(kvModel(), history, 5*time.Second)
	return res
}

func record(mu *sync.Mutex, history *[]porcupine.Operation, op porcupine.Operation) {
	mu.Lock()
	*history = append(*history, op)
	mu.Unlock()
}
