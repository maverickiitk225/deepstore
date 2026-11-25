# deepstore

A distributed key-value store. Three or more processes form a Raft cluster and serve Put, Get, Delete, and compare-and-swap over HTTP. The leader writes the log. A follower returns the leader's HTTP address, and the client retries there.

## Run

```
./scripts/cluster.sh
./scripts/cluster.sh 5
```

The first command starts three processes. The number starts that many. The script writes `name=ada` and reads it back. Ctrl-C stops them. Data stays in `.cluster/`.

To start the processes yourself, pass `-bootstrap` on the first process only:

```
go run ./cmd/deepstore serve -data-dir ./data1 -listen 127.0.0.1:7000 -raft-addr 127.0.0.1:7001 -node-id node1 -bootstrap
```

Wait until it prints `listening`, then start the other processes:

```
go run ./cmd/deepstore serve -data-dir ./data2 -listen 127.0.0.1:7002 -raft-addr 127.0.0.1:7003 -node-id node2 -join http://127.0.0.1:7000
go run ./cmd/deepstore serve -data-dir ./data3 -listen 127.0.0.1:7004 -raft-addr 127.0.0.1:7005 -node-id node3 -join http://127.0.0.1:7000
```

Each process has its own data directory, node id, Raft port, and HTTP port. `-node-id` and `-raft-addr` stay the same for that directory. `-raft-addr` must include an IP.

```
go run ./cmd/deepstore put name ada
go run ./cmd/deepstore cas name ada grace
go run ./cmd/deepstore get name
go run ./cmd/deepstore delete name
```

Client commands use `http://127.0.0.1:7000` unless you pass `-addr`.

| Method | Path | Request | Response |
| --- | --- | --- | --- |
| `PUT` | `/v1/keys/{key}` | `{"value":"<base64>","client_id":N,"seq":N}` | `{"index":N}` |
| `GET` | `/v1/keys/{key}` | | `{"value":"<base64>"}`, or 404 |
| `DELETE` | `/v1/keys/{key}` | `{"client_id":N,"seq":N}` | `{"index":N}` |
| `POST` | `/v1/keys/{key}/cas` | `{"expected":"<base64>","value":"<base64>","client_id":N,"seq":N}` | `{"index":N,"swapped":true}` |
| `POST` | `/v1/join` | `{"id":"...","raft_addr":"ip:port","http_addr":"http://host:port"}` | 204 |

`value` and `expected` are standard base64. The stored bytes are the decoded value, including bytes that are not valid UTF-8. A value that is not valid base64 is a 400.

A follower returns `503` and `{"error":"node: not leader","leader":"http://host:port"}`.

## Writes

The leader appends the command. It answers after a majority of voters have stored it. Compare-and-swap commits either way. The value changes only when the key is present and equals `expected`.

Each write has a client id and a sequence. Sequence 1 is that client's first command. The next command uses the next sequence. Repeating the latest sequence returns the original result. Any other sequence is rejected.

## Restart

Restart a process with the same data directory, node id, and Raft address. The directory holds `raft-log.db`, `raft-stable.db`, and `raft-snap`. With three voters, the cluster still serves if one process is down.

Snapshots run every 1024 log entries. `serve -snapshot-every 0` turns that off.

## Layout

| Package | Role |
| --- | --- |
| `internal/node` | One Raft server. |
| `internal/node/convert` | Bytes for log entries and the HTTP-address list. |
| `internal/server` | HTTP API. |
| `internal/client` | Client. Retries on the leader. |
| `internal/store` | In-memory map. |
| `cmd/deepstore` | `serve` and the client commands. |

`internal/engine` and `internal/wal` are the old single-process path. `serve` does not use them.

- [docs/durability.md](docs/durability.md)
- [docs/snapshot.md](docs/snapshot.md)
- [docs/node.md](docs/node.md)
- [cmd/README.md](cmd/README.md)
