# deepstore

deepstore is a distributed key-value store written in Go. A cluster of nodes replicates data with the Raft consensus algorithm ([hashicorp/raft](https://github.com/hashicorp/raft)) and stores the Raft log in BoltDB ([raft-boltdb](https://github.com/hashicorp/raft-boltdb)). Clients use a JSON HTTP API or the bundled CLI.

## Features

- **Linearizable reads and writes.** Every operation is served by the leader. A write succeeds only after a majority of nodes have stored it, and a read confirms leadership through Raft before it returns.
- **Fault tolerance.** A cluster of 2f + 1 nodes keeps serving with up to f nodes down. For example, 3 nodes tolerate 1 failure and 5 nodes tolerate 2.
- **Operations:** `put`, `get`, `delete`, and `cas` (compare-and-swap).
- **Exactly-once writes.** Each write carries a client ID and sequence number. A retried write is applied once, and the retry gets the original result.
- **Automatic leader redirect.** Followers reply with the leader's address, and the client retries there.
- **Snapshots and log compaction**, configurable with `-snapshot-every`.
- **Observability.** `/v1/status`, `/healthz`, `/readyz`, and Prometheus `/metrics`.

## Requirements

- Go 1.26 or later
- macOS or Linux

## Quick start

Start a local 3-node cluster:

```sh
./scripts/cluster.sh        # 3 nodes
./scripts/cluster.sh 5      # 5 nodes
```

The script builds the binary, starts the nodes, writes a test key, and reads it back. Node N listens for HTTP on port `7000 + 2(N-1)` and for Raft on the next port. Node 1 uses ports 7000 and 7001, and node 2 uses 7002 and 7003. Data and logs are kept in `.cluster/`. Press Ctrl-C to stop the cluster.

Then, in another terminal:

```sh
go run ./cmd/deepstore put name ada
go run ./cmd/deepstore get name                # ada
go run ./cmd/deepstore cas name ada grace      # prints "<index> true"
go run ./cmd/deepstore delete name
```

## Running a cluster manually

Build the binary:

```sh
go build -o deepstore ./cmd/deepstore
```

Start the first node with `-bootstrap`. This creates a new cluster:

```sh
./deepstore serve -node-id node1 -data-dir ./data1 \
  -listen 127.0.0.1:7000 -raft-addr 127.0.0.1:7001 -bootstrap
```

When it prints `listening on ...`, start the other nodes with `-join` pointing at any running node:

```sh
./deepstore serve -node-id node2 -data-dir ./data2 \
  -listen 127.0.0.1:7002 -raft-addr 127.0.0.1:7003 -join http://127.0.0.1:7000

./deepstore serve -node-id node3 -data-dir ./data3 \
  -listen 127.0.0.1:7004 -raft-addr 127.0.0.1:7005 -join http://127.0.0.1:7000
```

Rules:

- Each node needs a unique `-node-id`, its own `-data-dir`, and its own ports.
- Use `-bootstrap` on one node only, and only the first time. A data directory that already holds cluster state never bootstraps again.
- `-raft-addr` must be an IP address and port, not a hostname.
- An empty data directory with neither `-bootstrap` nor `-join` refuses to start.

See [cmd/README.md](cmd/README.md) for every flag.

## Cluster operations

### Restarting a node

Restart with the same `-data-dir`, `-node-id`, and `-raft-addr`. The node reloads its log and snapshot and rejoins the cluster. You can keep `-join` on the command line. A node that is already a member re-registers its HTTP address and continues.

### Data directory

| Path | Contents |
| --- | --- |
| `raft-log.db` | Raft log (BoltDB) |
| `raft-stable.db` | Raft term and vote (BoltDB) |
| `raft-snap/` | Snapshots. The two most recent are kept. |

A snapshot is taken every 1024 log entries by default. Change this with `-snapshot-every N`, or set it to `0` to turn automatic snapshots off.

### Node IDs and moving a node

A join is refused with HTTP 409, and the node exits, if its `-node-id` already belongs to another address in the cluster. This prevents a misconfigured node from taking over an existing member's identity.

If a node has really moved to a new address, or was lost and is being replaced, start it with `-join ... -replace` to move its ID to the new address. The current leader's ID cannot be replaced.

There is currently no command to remove a node from the cluster. See [docs/improvements.md](docs/improvements.md).

## HTTP API

All bodies are JSON. Values are **base64-encoded**, so any bytes can be stored.

| Method | Path | Request body | Success response |
| --- | --- | --- | --- |
| `PUT` | `/v1/keys/{key}` | `{"value": "<base64>", "client_id": N, "seq": N}` | `{"index": N}` |
| `GET` | `/v1/keys/{key}` | | `{"value": "<base64>"}` |
| `DELETE` | `/v1/keys/{key}` | `{"client_id": N, "seq": N}` | `{"index": N}` |
| `POST` | `/v1/keys/{key}/cas` | `{"expected": "<base64>", "value": "<base64>", "client_id": N, "seq": N}` | `{"index": N, "swapped": true}` |
| `POST` | `/v1/join` | `{"id": "...", "raft_addr": "ip:port", "http_addr": "http://host:port", "replace": false}` | `204 No Content` |
| `GET` | `/v1/status` | | Node status (see below) |
| `GET` | `/healthz` | | `{"ok": true}` |
| `GET` | `/readyz` | | `{"ok": true}` |
| `GET` | `/metrics` | | Prometheus text format |

`index` is the Raft log index at which the write was committed.

### Example

```sh
# Store "hello" (base64 "aGVsbG8=")
curl -X PUT localhost:7000/v1/keys/greeting \
  -d '{"value":"aGVsbG8=","client_id":42,"seq":1}'
# {"index":6}

curl localhost:7000/v1/keys/greeting
# {"value":"aGVsbG8="}
```

### Client IDs and sequence numbers

Every write (`PUT`, `DELETE`, `cas`) must include a non-zero `client_id` and `seq`:

- A client picks a random 64-bit `client_id` and starts at `seq: 1`.
- Each new write uses the next sequence number.
- Resending the **same** `seq`, for example after a timeout, returns the original result without applying the write again.
- Any other sequence is rejected with `400`.

The Go client (`internal/client`) handles this for you. It also retries a write with the same sequence when the outcome is uncertain, such as a lost response or a leader change.

### Compare-and-swap

`cas` sets the key to `value` only if the key exists and its current value equals `expected`. The response reports `"swapped": true` or `false`. A CAS on a missing key never succeeds. Both outcomes use a log entry and return an `index`.

### Status codes

| Code | Meaning |
| --- | --- |
| `200` / `204` | Success |
| `400` | Malformed request, invalid base64, or wrong sequence number |
| `404` | Key not found |
| `409` | Join refused: the node ID belongs to another address |
| `500` | Internal error, such as a Raft timeout. The write may or may not have been applied. Retry with the same `seq`. |
| `503` | This node is not the leader. The body includes the leader's address if known: `{"error": "node: not leader", "leader": "http://127.0.0.1:7000"}` |

### Monitoring endpoints

These endpoints report on the node that receives the request. They never redirect to the leader and never write to the Raft log.

- **`/healthz`** returns `200` while the process is serving HTTP. Use it as a liveness probe.
- **`/readyz`** returns `200` when the node is the leader, or is a follower that knows the leader, has heard from it in the last 3 seconds, and is no more than 64 entries behind the commit index. Otherwise it returns `503` with a reason, such as `{"ok":false,"reason":"no leader"}`. Use it as a readiness probe.
- **`/v1/status`** returns details for debugging:

  ```json
  {
    "id": "node1", "state": "Leader", "term": 2,
    "leader_id": "node1", "leader": "http://127.0.0.1:7000",
    "commit_index": 8, "applied_index": 8, "last_log_index": 8, "last_snapshot_index": 0,
    "keys": 1, "sessions": 1,
    "peers": [{"id": "node1", "raft_addr": "127.0.0.1:7001", "http_addr": "http://127.0.0.1:7000", "suffrage": "Voter"}]
  }
  ```

- **`/metrics`** exposes Raft's built-in metrics plus `deepstore_raft_commit_index`, `deepstore_raft_applied_index`, `deepstore_raft_apply_lag`, `deepstore_raft_term`, `deepstore_raft_leader`, `deepstore_keys`, `deepstore_sessions`, and HTTP request counts and latency by route and status code.

## How it works

1. A client sends a request to any node. A follower replies `503` with the leader's address, and the client retries there.
2. **Writes.** The leader appends the command to the Raft log and replicates it. Once a majority has stored it, the command is committed and applied to the in-memory key-value map on every node. The leader then responds.
3. **Reads.** The leader issues a Raft barrier. This confirms it is still leader and that every earlier write has been applied, so the read always sees the latest committed value.
4. **Sessions.** The state machine stores each client's last sequence number and result. Retried writes are detected and answered from that record.
5. **Snapshots.** Periodically the map, the session table, and the node address list are written to a snapshot, and older log entries are discarded.

## Project layout

| Path | Description |
| --- | --- |
| `cmd/deepstore` | The `deepstore` binary: server and CLI commands |
| `internal/node` | Raft node: setup, joining, the replicated state machine (FSM), and status |
| `internal/node/convert` | Binary encoding of log entries and the node address list |
| `internal/server` | HTTP API, health checks, and metrics |
| `internal/client` | Go client with leader redirect and write retries |
| `internal/store` | In-memory key-value map and client session table |
| `internal/snapshot` | Snapshot encoding |
| `internal/engine`, `internal/wal` | Earlier single-node storage engine. Not used by `serve`. |
| `test/linearizable` | Linearizability tests using [porcupine](https://github.com/anishathalye/porcupine) |
| `scripts/cluster.sh` | Starts a local cluster |

## Testing

```sh
go test ./...
go test -race ./...
```

`test/linearizable` records concurrent client histories, including runs where the server is killed with `kill -9` and restarted, and checks that the history is linearizable.

## Limitations

- The whole dataset is held in memory, and a snapshot is limited to 1 GiB.
- Every read goes through the Raft log, so reads cost about as much as writes.
- There is no TLS or authentication, and `/v1/join` is open to anyone who can reach the HTTP port.
- There is no command to remove a node from the cluster.
- The session table is never pruned.

[docs/improvements.md](docs/improvements.md) lists known issues and planned work. Design notes for the earlier single-node engine are in [docs/durability.md](docs/durability.md), [docs/snapshot.md](docs/snapshot.md), and [docs/node.md](docs/node.md).

## License

See [LICENSE](LICENSE).
