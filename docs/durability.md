# Durability

`internal/store` is the in-memory map. `internal/wal` is `wal.log` in the data directory. `internal/engine` appends a record, calls `Sync`, applies the command, then returns. Get reads the map.

## Record format

Each record is one command. Integers are unsigned and little-endian. The CRC is the IEEE CRC of `length || payload`.

```
[ crc32:4B ][ length:4B ][ payload ]

payload v2:
[ version:1B ][ index:8B ][ op:1B ][ key_len:4B ][ key ][ val_len:4B ][ value ]
[ exp_len:4B ][ expected ]          // CAS only

payload v3, inserted after index:
[ client_id:8B ][ seq:8B ]
```

| Field | Rule |
| --- | --- |
| `version` | `2` has no client id or sequence. `3` requires both, non-zero. The engine writes version 3. Any other version fails open. |
| `index` | First record is 1. Each record is the previous index plus 1. |
| `op` | `1` Put, `2` Delete, `3` CAS. Delete has `val_len` 0. |
| `key` | Non-empty. Copied out of the read buffer on decode. |
| `value` | Put and CAS payload. Empty is a stored value. Copied out of the read buffer. |
| `expected` | CAS only, after `value`. Copied out of the read buffer. |
| payload length | Above 16 MiB fails open. |

CAS appends the record in either case. The map changes when the key is present and its value equals `expected`. A missing key leaves the map unchanged, including a compare with `""`. A stored empty string matches `expected` `""`. The index advances either way. Sessions for version 3 are in [node.md](node.md).

## Sync

`write` copies bytes into the kernel cache. Durability is `File.Sync`, which is `F_FULLFSYNC` on darwin. Several records share one `Sync`; the batching is in [node.md](node.md). The caller returns after that `Sync` has returned nil and the command has been applied.

A `Sync` error returns that error to every caller in the batch. Those commands are not applied. Later writes fail with a poisoned engine until the process exits. The next open replays the bytes that are in the file.

Creating `wal.log` is followed by an fsync of the parent directory.

## Open

`Open` creates the data directory and `wal.log`, or opens the existing file, then reads from offset 0 onto an empty map.

| Input | Result |
| --- | --- |
| Valid CRC, length, version, and `index == previous + 1` | Applied |
| Short read at the end of the file | Torn tail. Truncate to the last good offset. |
| Bad CRC, and no valid record follows | Torn tail. Truncate to the last good offset. |
| Bad CRC followed by a valid record | Open fails |
| Index gap or repeated index | Open fails |
| Unknown version, or length above 16 MiB | Open fails |

A torn tail includes a full length whose payload never landed. After truncation, later appends start at the last good offset. During open, replay is the only update to the map.

## Acknowledgement

A successful write means the record was covered by a successful `Sync` and the map includes that command. After a restart, Get returns that value or a later successful write to the same key.

Writes to one key are applied in index order.

A crash before the response leaves the command either in the file or absent. The client retries with the same sequence, described in [node.md](node.md).

## Tests

```
go test ./internal/store ./internal/wal ./internal/engine
```

| Coverage | Test |
| --- | --- |
| Put, Delete, missing key, concurrent updates, Get alongside Put | `internal/store` |
| Append, replay, one `write` per batch | `TestWALAppendReplay`, `TestWALAppendManyOneWrite` |
| Torn tail and a bad CRC on the last record | `TestWALTornTailTruncates`, `TestWALBadCRCOnFinalRecordTruncates` |
| Bad CRC before a valid record, oversized length, index gap, repeated index, unknown version | `internal/wal` |
| Reopen after Put, Delete, and a torn tail | `TestEnginePutGetReopen`, `TestEngineDeleteReopen`, `TestEngineTornTailAfterReopen` |
| Failed `Sync` poisons the process; reopen replays the bytes in the file | `TestEngineSyncFailurePoisons` |

To check a crash by hand: `serve` a data directory, `put` a key, `kill -9` the server, `serve` the same directory, `get` the key.
