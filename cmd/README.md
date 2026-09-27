# cmd

`cmd/deepstore` is the process you run and kill by hand. Add it only after WAL replay tests pass (see [docs/phase1-engine.md](../docs/phase1-engine.md)).

Target shape, not an implementation:

- One binary: `go run ./cmd/deepstore`
- Flag for a data directory (where `wal.log` lives)
- Enough surface to Put / Get / Delete — a tiny CLI or a few HTTP routes
- After an acked Put, `kill -9` the process, start it again on the same dir, Get must see the value

No gRPC, no cluster flags, no Raft in this binary yet.
