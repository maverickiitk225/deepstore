# deepstore

A key-value store for learning how a durable, linearizable node works. The end goal is a small distributed system. What runs today is one process: an in-memory map, an append-only log, and an HTTP server.

## Run

```
go run ./cmd/deepstore serve -data-dir ./data
go run ./cmd/deepstore put name ada
go run ./cmd/deepstore cas name ada grace
go run ./cmd/deepstore get name
go run ./cmd/deepstore delete name
```

`serve` listens on `:7000` unless you pass `-listen`. The client commands talk to `http://127.0.0.1:7000`.

| Method | Path | Request | Response |
| --- | --- | --- | --- |
| `PUT` | `/v1/keys/{key}` | `{"value":"...","client_id":N,"seq":N}` | `{"index":N}` |
| `GET` | `/v1/keys/{key}` | | `{"value":"..."}`, or 404 |
| `DELETE` | `/v1/keys/{key}` | `{"client_id":N,"seq":N}` | `{"index":N}` |
| `POST` | `/v1/keys/{key}/cas` | `{"expected":"...","value":"...","client_id":N,"seq":N}` | `{"index":N,"swapped":true}` |

## A write

One writer drains the queue, appends the batch, calls `Sync` once, then applies each record in index order. Several writes share that fsync. `write` only copies bytes into the kernel cache. Compare-and-swap uses that same path: the record is appended either way, and the map changes only when the key is present and equals `expected`. A miss still advances the index. `swapped` is false and the value stays as it was.

Each write carries a client id and a sequence number. The id is chosen by the client. Sequence 1 is that client's first command, and each new command adds one. The state machine remembers the latest sequence and its result. Sending that sequence again returns the original index and the original `swapped` flag, and does not append. The CLI picks a new id per process and uses sequence 1 for its single command.

If `Sync` fails, nothing in that batch is applied. Later writes fail until the process is restarted, and restart replays whatever is actually in the file.

## Recovery

Each record is a CRC32, a length, and a versioned payload: index, operation, key, value, and for compare-and-swap the expected value. Indices start at 1 and increase by one. On open, the log is replayed onto an empty map.

A torn tail is truncated back to the last good record. That is a short read at the end of the file, or a bad checksum with no valid record after it. A bad checksum followed by a valid record, an index gap or repeat, an unknown version, or a length past the maximum is corruption, and open fails.

Creating `wal.log` is followed by an fsync of the parent directory, so the new directory entry survives a crash too.

The log index and the applied index are equal after every acknowledgement. They are tracked separately because a replicated log will have a commit index between them. Concurrent histories, including ones that kill the server and restart it on the same directory, are checked with [Porcupine](https://github.com/anishathalye/porcupine). A call that timed out or lost its connection is an unknown outcome.

## Layout

| Package | Role |
| --- | --- |
| `internal/store` | In-memory map. Applies one record. No disk. |
| `internal/wal` | Append-only file, checksums, `Sync`, replay. |
| `internal/engine` | Queues writes, group-commits, applies, then acks. |
| `internal/server` | HTTP API over the engine. |
| `cmd/deepstore` | `serve`, plus a small client. |

Snapshots and more than one node are still ahead.

- [docs/phase1-engine.md](docs/phase1-engine.md) — record layout, fsync policy, replay rules
- [docs/phase2-node.md](docs/phase2-node.md) — the node this is growing into
- [cmd/README.md](cmd/README.md) — flags and routes
