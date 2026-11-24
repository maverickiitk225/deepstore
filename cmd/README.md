# cmd

`cmd/deepstore` is the process you run.

```
deepstore serve -data-dir DIR [-listen ADDR] [-node-id ID] [-raft-addr IP:PORT] [-join URL] [-snapshot-every N]
deepstore put [-addr URL] KEY VALUE
deepstore get [-addr URL] KEY
deepstore delete [-addr URL] KEY
deepstore cas [-addr URL] KEY EXPECTED VALUE
```

`serve` opens a Raft node and keeps the HTTP API up until SIGINT or SIGTERM. With no `-join`, an empty directory becomes a one-voter cluster and HTTP starts once that process is leader. A later start of that same one-voter directory does the same. `-join` is the HTTP address of a node already in the cluster; the new process is added as a voter and serves immediately. A restarted member of a larger cluster serves even when it is a follower. Shutdown stops accepting, finishes in-flight requests, and shuts Raft down. `-node-id` defaults to `node1` and is fixed for the data directory. `-raft-addr` defaults to `127.0.0.1:7001` and has to include an IP. Use a specific `-listen` host when other machines will follow the leader URL. `-snapshot-every` is how many Raft log entries sit between snapshots. The default is 1024. `0` does not snapshot on its own. `put`, `get`, `delete`, and `cas` talk to any server in the cluster. A follower responds with the leader's HTTP address and the client retries there. Each process picks its own client id and sends sequence 1. The default address is `http://127.0.0.1:7000`.

Routes:

- `PUT /v1/keys/{key}` with `{"value":"...","client_id":N,"seq":N}` returns `{"index":N}`
- `GET /v1/keys/{key}` returns `{"value":"..."}`, or 404
- `DELETE /v1/keys/{key}` with `{"client_id":N,"seq":N}` returns `{"index":N}`
- `POST /v1/keys/{key}/cas` with `{"expected":"...","value":"...","client_id":N,"seq":N}` returns `{"index":N,"swapped":true}`
- `POST /v1/join` with `{"id":"...","raft_addr":"ip:port","http_addr":"http://host:port"}` returns 204 from the leader

`cas` prints the index and `true` or `false`. A false result is a completed compare that left the key unchanged.
