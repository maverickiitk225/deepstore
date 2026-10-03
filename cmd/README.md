# cmd

`cmd/deepstore` is the process you run.

```
deepstore serve -data-dir DIR [-listen ADDR]
deepstore put [-addr URL] KEY VALUE
deepstore get [-addr URL] KEY
deepstore delete [-addr URL] KEY
deepstore cas [-addr URL] KEY EXPECTED VALUE
```

`serve` keeps the HTTP API up until SIGINT or SIGTERM, then stops accepting, finishes in-flight requests, and closes the WAL. `put`, `get`, and `delete` talk to that server. The default address is `http://127.0.0.1:7000`.

Routes:

- `PUT /v1/keys/{key}` with `{"value":"..."}` returns `{"index":N}`
- `GET /v1/keys/{key}` returns `{"value":"..."}`, or 404
- `DELETE /v1/keys/{key}` returns `{"index":N}`
- `POST /v1/keys/{key}/cas` with `{"expected":"...","value":"..."}` returns `{"index":N,"swapped":true}`

`cas` prints the index and `true` or `false`. A false result is a completed compare that left the key unchanged.
