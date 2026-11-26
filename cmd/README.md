# deepstore command reference

`cmd/deepstore` builds the `deepstore` binary. It runs a cluster node (`serve`) and provides client commands (`put`, `get`, `delete`, `cas`).

```sh
go build -o deepstore ./cmd/deepstore
```

## Commands

```
deepstore serve  -data-dir DIR [flags]
deepstore put    [-addr URL] KEY VALUE
deepstore get    [-addr URL] KEY
deepstore delete [-addr URL] KEY
deepstore cas    [-addr URL] KEY EXPECTED VALUE
```

## `serve`

Starts a node and serves the HTTP API until it receives SIGINT or SIGTERM.

### Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-data-dir` | (required) | Directory for the Raft log, Raft state, and snapshots |
| `-node-id` | `node1` | Unique ID of this node. Must stay the same for a data directory. |
| `-listen` | `:7000` | HTTP listen address. Use a specific host if other machines need to reach this node through leader redirects. |
| `-raft-addr` | `127.0.0.1:7001` | Raft address. Must be an IP and port. Peers connect to this address. |
| `-bootstrap` | `false` | Create a new single-node cluster if the data directory is empty |
| `-join` | (none) | HTTP address of any node in an existing cluster to join |
| `-replace` | `false` | With `-join`: move this node ID to `-raft-addr` if the cluster has it at a different address |
| `-snapshot-every` | `1024` | Number of log entries between snapshots. `0` turns automatic snapshots off. |

### Startup behavior

| Data directory | Flags | Result |
| --- | --- | --- |
| Empty | `-bootstrap` | Creates a new cluster with this node as leader |
| Empty | `-join URL` | Joins the existing cluster as a voter |
| Empty | `-bootstrap -join URL` | Joins if `URL` is reachable. Otherwise exits, so a second cluster is not created by mistake. |
| Empty | neither | Exits with an error |
| Has cluster state | any | Rejoins using the stored state. Never bootstraps again. |

If `-join` is set and the cluster already has this node ID at a different Raft address, the join is refused and the process exits. Pass `-replace` only when the node has really moved or the old one is gone for good.

On shutdown the node stops accepting requests, waits up to 15 seconds for in-flight requests to finish, and then stops Raft.

## Client commands

`put`, `get`, `delete`, and `cas` send a request to `-addr`, which defaults to `http://127.0.0.1:7000`. Any node works. A follower redirects the request to the leader.

| Command | Output on success |
| --- | --- |
| `put KEY VALUE` | Log index of the write |
| `get KEY` | The value. A missing key prints nothing and exits with 1. |
| `delete KEY` | Log index of the write |
| `cas KEY EXPECTED VALUE` | Log index and `true` if the value was swapped, `false` if not |

Each command gives up after 30 seconds. Writes that fail with an uncertain outcome, such as a lost response or a leader change, are retried within that time with the same sequence number, so they are applied at most once.

Each CLI invocation uses a new client ID. Running the same command twice therefore applies it twice.

### Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success |
| `1` | Request failed, or `get` found no key |
| `2` | Invalid arguments |

## HTTP API

The API and the monitoring endpoints are documented in the [main README](../README.md#http-api).
