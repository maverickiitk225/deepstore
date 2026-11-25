package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/deepanker/deepstore/internal/client"
	"github.com/deepanker/deepstore/internal/errs"
	"github.com/deepanker/deepstore/internal/node"
	"github.com/deepanker/deepstore/internal/server"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		os.Exit(serve(os.Args[2:]))
	case "put":
		os.Exit(putCmd(os.Args[2:]))
	case "get":
		os.Exit(getCmd(os.Args[2:]))
	case "delete":
		os.Exit(deleteCmd(os.Args[2:]))
	case "cas":
		os.Exit(casCmd(os.Args[2:]))
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(2)
	}
}

func serve(args []string) int {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	dataDir := fs.String("data-dir", "", "directory for the raft log and snapshots")
	listen := fs.String("listen", ":7000", "address to listen on")
	nodeID := fs.String("node-id", "node1", "stable raft id for this data directory")
	raftAddr := fs.String("raft-addr", "127.0.0.1:7001", "raft bind address, including an IP")
	bootstrap := fs.Bool("bootstrap", false, "form a new cluster when the data directory is empty")
	join := fs.String("join", "", "http address of a node in an existing cluster")
	every := fs.Uint64("snapshot-every", node.DefaultSnapshotThreshold, "raft log entries between snapshots (0 disables)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: deepstore serve -data-dir DIR [-listen ADDR] [-node-id ID] [-raft-addr IP:PORT] [-bootstrap] [-join URL] [-snapshot-every N]\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "error: -data-dir is required")
		fs.Usage()
		return 2
	}

	has, err := node.HasExistingState(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		return 1
	}
	peerReachable := false
	if !has && *bootstrap && *join != "" {
		peerReachable, err = joinPeerReachable(*join)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 2
		}
		if peerReachable {
			fmt.Fprintf(os.Stderr, "-bootstrap ignored: %s is reachable\n", *join)
		}
	}
	doBootstrap, err := resolveBootstrap(has, *bootstrap, *join, peerReachable)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 2
	}

	nd, err := node.Open(node.Config{
		Dir:               *dataDir,
		NodeID:            *nodeID,
		RaftAddr:          *raftAddr,
		Bootstrap:         doBootstrap,
		SnapshotThreshold: *every,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		return 1
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	httpAddr := advertiseURL(*listen)
	if *join != "" {
		fmt.Fprintf(os.Stderr, "joining %s\n", *join)
		if err := joinCluster(ctx, nd, *join, *nodeID, httpAddr); err != nil {
			fmt.Fprintf(os.Stderr, "join: %v\n", err)
			_ = nd.Close()
			return 1
		}
		go advertiseWhenLeader(ctx, nd, httpAddr)
	} else if nd.VoterCount() <= 1 {
		if err := nd.WaitForLeader(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "leader: %v\n", err)
			_ = nd.Close()
			return 1
		}
		if err := nd.Advertise(httpAddr); err != nil {
			fmt.Fprintf(os.Stderr, "advertise: %v\n", err)
			_ = nd.Close()
			return 1
		}
	} else {
		go advertiseWhenLeader(ctx, nd, httpAddr)
	}

	srv := server.New(nd, *listen)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	fmt.Fprintf(os.Stderr, "listening on %s\n", *listen)

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			_ = nd.Close()
			return 1
		}
	case <-ctx.Done():
		stop()
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
			_ = nd.Close()
			return 1
		}
	}
	if err := nd.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		return 1
	}
	return 0
}

// resolveBootstrap reports whether this process may form a new one-node cluster.
// An existing data directory never bootstraps. An empty one bootstraps only when
// wantBootstrap is set and join is empty. When both are set, a reachable join
// address is joined instead, and an unreachable one refuses to start.
func resolveBootstrap(hasState, wantBootstrap bool, join string, peerReachable bool) (bool, error) {
	if hasState || (join != "" && !wantBootstrap) {
		return false, nil
	}
	if join == "" {
		if !wantBootstrap {
			return false, errors.New("data directory has no cluster state: pass -bootstrap to create a cluster, or -join to join one")
		}
		return true, nil
	}
	if peerReachable {
		return false, nil
	}
	return false, errors.New("refusing -bootstrap because the join peer is not reachable. Omit -bootstrap to wait for it, or omit -join to create a new cluster")
}

func joinPeerReachable(raw string) (bool, error) {
	target := strings.TrimSpace(raw)
	if target == "" {
		return false, errors.New("join address is empty")
	}
	if !strings.Contains(target, "://") {
		target = "http://" + target
	}
	u, err := url.Parse(target)
	if err != nil {
		return false, fmt.Errorf("join address: %w", err)
	}
	if _, _, err := net.SplitHostPort(u.Host); err != nil {
		return false, fmt.Errorf("join address %q needs a host and port", raw)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 2*time.Second)
	if err != nil {
		return false, nil
	}
	_ = conn.Close()
	return true, nil
}

func joinCluster(ctx context.Context, nd *node.Node, leader, id, httpAddr string) error {
	if nd.HasExistingState() {
		joinCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := node.Join(joinCtx, leader, id, nd.RaftAddr(), httpAddr); err != nil {
			fmt.Fprintf(os.Stderr, "join: %v\n", err)
		}
		return nil
	}
	return node.Join(ctx, leader, id, nd.RaftAddr(), httpAddr)
}

func advertiseWhenLeader(ctx context.Context, nd *node.Node, httpAddr string) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		err := nd.Advertise(httpAddr)
		if err == nil {
			return
		}
		if !errors.Is(err, errs.ErrNotLeader) {
			fmt.Fprintf(os.Stderr, "advertise: %v\n", err)
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func advertiseURL(listen string) string {
	listen = strings.TrimSpace(listen)
	if strings.Contains(listen, "://") {
		u, err := url.Parse(listen)
		if err != nil || u.Host == "" {
			return strings.TrimRight(listen, "/")
		}
		u.Path, u.RawQuery, u.Fragment = "", "", ""
		return strings.TrimRight(u.String(), "/")
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return "http://" + strings.TrimRight(listen, "/")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port)
}

func putCmd(args []string) int {
	fs, addr := clientFlags("put")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: deepstore put [-addr URL] KEY VALUE\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 2 {
		fs.Usage()
		return 2
	}
	index, err := client.New(*addr).Put(context.Background(), fs.Arg(0), fs.Arg(1))
	if err != nil {
		fmt.Fprintf(os.Stderr, "put: %v\n", err)
		return 1
	}
	fmt.Println(index)
	return 0
}

func getCmd(args []string) int {
	fs, addr := clientFlags("get")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: deepstore get [-addr URL] KEY\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	value, ok, err := client.New(*addr).Get(context.Background(), fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "get: %v\n", err)
		return 1
	}
	if !ok {
		return 1
	}
	fmt.Println(value)
	return 0
}

func deleteCmd(args []string) int {
	fs, addr := clientFlags("delete")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: deepstore delete [-addr URL] KEY\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 1 {
		fs.Usage()
		return 2
	}
	index, err := client.New(*addr).Delete(context.Background(), fs.Arg(0))
	if err != nil {
		fmt.Fprintf(os.Stderr, "delete: %v\n", err)
		return 1
	}
	fmt.Println(index)
	return 0
}

func casCmd(args []string) int {
	fs, addr := clientFlags("cas")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: deepstore cas [-addr URL] KEY EXPECTED VALUE\n\n")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 3 {
		fs.Usage()
		return 2
	}
	index, swapped, err := client.New(*addr).CAS(context.Background(), fs.Arg(0), fs.Arg(1), fs.Arg(2))
	if err != nil {
		fmt.Fprintf(os.Stderr, "cas: %v\n", err)
		return 1
	}
	fmt.Printf("%d %t\n", index, swapped)
	return 0
}

func clientFlags(name string) (*flag.FlagSet, *string) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	addr := fs.String("addr", "http://127.0.0.1:7000", "server address")
	return fs, addr
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  deepstore serve -data-dir DIR [-listen ADDR] [-node-id ID] [-raft-addr IP:PORT] [-bootstrap] [-join URL] [-snapshot-every N]
  deepstore put [-addr URL] KEY VALUE
  deepstore get [-addr URL] KEY
  deepstore delete [-addr URL] KEY
  deepstore cas [-addr URL] KEY EXPECTED VALUE
`)
}
