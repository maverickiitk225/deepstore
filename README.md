# deepstore

A durable, linearizable key-value store. Acknowledged writes stay, retries apply once, and a Raft cluster serves Put, Get, Delete, and compare-and-swap over HTTP.

## Run

```
go run ./cmd/deepstore serve -data-dir ./data
go run ./cmd/deepstore put name ada
go run ./cmd/deepstore cas name ada grace
go run ./cmd/deepstore get name
go run ./cmd/deepstore delete name
```

`serve` listens on `:7000` unless you pass `-listen`. Raft binds `127.0.0.1:7001` unless you pass `-raft-addr` (the address has to include an IP). `-node-id` defaults to `node1` and stays fixed for that data directory. An empty directory with no `-join` bootstraps a one-voter cluster. Further processes join that cluster:

```
go run ./cmd/deepstore serve -data-dir ./data2 -listen 127.0.0.1:7002 -raft-addr 127.0.0.1:7003 -node-id node2 -join http://127.0.0.1:7000
go run ./cmd/deepstore serve -data-dir ./data3 -listen 127.0.0.1:7004 -raft-addr 127.0.0.1:7005 -node-id node3 -join http://127.0.0.1:7000
```

Each process has its own data directory, node id, Raft address, and HTTP address. A follower serves HTTP and names the leader. The client retries there. Three voters keep the cluster available when one process is down. The client commands talk to `http://127.0.0.1:7000`.

| Method | Path | Request | Response |
| --- | --- | --- | --- |
| `PUT` | `/v1/keys/{key}` | `{"value":"...","client_id":N,"seq":N}` | `{"index":N}` |
| `GET` | `/v1/keys/{key}` | | `{"value":"..."}`, or 404 |
| `DELETE` | `/v1/keys/{key}` | `{"client_id":N,"seq":N}` | `{"index":N}` |
| `POST` | `/v1/keys/{key}/cas` | `{"expected":"...","value":"...","client_id":N,"seq":N}` | `{"index":N,"swapped":true}` |
| `POST` | `/v1/join` | `{"id":"...","raft_addr":"ip:port","http_addr":"http://host:port"}` | 204 |

## A write

The server proposes the command to the Raft log. A majority of the voters have to store it before the leader answers. After the entry commits, the state machine sets the command index from the Raft log index and applies it. Compare-and-swap uses that same path: the entry is committed either way, and the map changes only when the key is present and equals `expected`. A miss still advances the index. `swapped` is false and the value stays as it was. A follower answers 503 and includes the leader's HTTP address.

Each write carries a client id and a sequence number. The id is chosen by the client. Sequence 1 is that client's first command, and each new command adds one. The state machine remembers the latest sequence and its result. Sending that sequence again returns the original index and the original `swapped` flag, and does not propose. Any other sequence is rejected. The CLI picks a new id per process and uses sequence 1 for its single command.

A get runs on the leader after a barrier, so the map includes every command committed before the read.

## Recovery

`raft-log.db` and `raft-stable.db` in the data directory are the Raft log. Snapshots are under `raft-snap`. Restart opens the same directory with the same node id and Raft address, restores the latest snapshot, and replays the tail. A command that was acknowledged is in that state.

Concurrent histories, including ones that kill the server and restart it on the same directory, are checked with [Porcupine](https://github.com/anishathalye/porcupine). A call that timed out or lost its connection is an unknown outcome.

## Layout

| Package | Role |
| --- | --- |
| `internal/store` | In-memory map. Applies one record. No disk. |
| `internal/wal` | Append-only file, checksums, `Sync`, replay. |
| `internal/snapshot` | Checksummed checkpoint of the map and sessions. |
| `internal/node` | Raft node. Proposes commands, adds voters, and applies them through the state machine. |
| `internal/engine` | Single-node commit path. `serve` does not call it. |
| `internal/server` | HTTP API. |
| `cmd/deepstore` | `serve`, plus a small client. |

After the configured number of Raft log entries, the library writes a snapshot and compacts the prefix. The default is 1024. `serve -snapshot-every 0` leaves the log whole. The state-machine snapshot frame is the one in [docs/snapshot.md](docs/snapshot.md).

- [docs/durability.md](docs/durability.md) — durable log, replay, and what an ack means
- [docs/snapshot.md](docs/snapshot.md) — snapshot file, log compaction, and recovery
- [docs/node.md](docs/node.md) — serve path, sessions, group commit, and the linearizability harness
- [cmd/README.md](cmd/README.md) — flags and routes
