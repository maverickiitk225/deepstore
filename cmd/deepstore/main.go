package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/deepanker/deepstore/internal/engine"
)

func main() {
	dataDir := flag.String("data-dir", "", "directory where wal.log is stored")
	flag.Usage = usage
	flag.Parse()

	if *dataDir == "" {
		fmt.Fprintln(os.Stderr, "error: -data-dir is required")
		usage()
		os.Exit(2)
	}

	args := flag.Args()
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "error: command required")
		usage()
		os.Exit(2)
	}

	e, err := engine.Open(*dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "open: %v\n", err)
		os.Exit(1)
	}
	defer e.Close()

	switch args[0] {
	case "put":
		if len(args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: deepstore -data-dir DIR put KEY VALUE")
			os.Exit(2)
		}
		if err := e.Put(args[1], args[2]); err != nil {
			fmt.Fprintf(os.Stderr, "put: %v\n", err)
			os.Exit(1)
		}
	case "get":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: deepstore -data-dir DIR get KEY")
			os.Exit(2)
		}
		val, ok := e.Get(args[1])
		if !ok {
			os.Exit(1)
		}
		fmt.Println(val)
	case "delete":
		if len(args) != 2 {
			fmt.Fprintln(os.Stderr, "usage: deepstore -data-dir DIR delete KEY")
			os.Exit(2)
		}
		if err := e.Delete(args[1]); err != nil {
			fmt.Fprintf(os.Stderr, "delete: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", args[0])
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
  deepstore -data-dir DIR put KEY VALUE
  deepstore -data-dir DIR get KEY
  deepstore -data-dir DIR delete KEY

Flags:
`)
	flag.PrintDefaults()
}
