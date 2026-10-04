package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/deepanker/deepstore/internal/client"
	"github.com/deepanker/deepstore/internal/engine"
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
	dataDir := fs.String("data-dir", "", "directory where wal.log is stored")
	listen := fs.String("listen", ":7000", "address to listen on")
	every := fs.Uint64("snapshot-every", engine.DefaultSnapshotEvery, "applied records between snapshots (0 disables)")
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: deepstore serve -data-dir DIR [-listen ADDR] [-snapshot-every N]\n\n")
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

	e, err := engine.Open(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		return 1
	}
	e.SetSnapshotEvery(*every)

	srv := server.New(e, *listen)
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.ListenAndServe()
	}()
	fmt.Fprintf(os.Stderr, "listening on %s\n", *listen)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintf(os.Stderr, "serve: %v\n", err)
			_ = e.Close()
			return 1
		}
	case <-ctx.Done():
		stop()
		shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			fmt.Fprintf(os.Stderr, "shutdown: %v\n", err)
			_ = e.Close()
			return 1
		}
	}
	if err := e.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "close: %v\n", err)
		return 1
	}
	return 0
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
  deepstore serve -data-dir DIR [-listen ADDR] [-snapshot-every N]
  deepstore put [-addr URL] KEY VALUE
  deepstore get [-addr URL] KEY
  deepstore delete [-addr URL] KEY
  deepstore cas [-addr URL] KEY EXPECTED VALUE
`)
}
