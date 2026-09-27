# Phase 1: single-node durable KV

This is the register the cluster must later impersonate. Consensus is out of scope until a Put that returned OK still exists after `kill -9`.

## Roles

| Piece | Job | Later becomes |
| --- | --- | --- |
| `internal/store` | In-memory `map` + lock. Apply one record. No disk. | Raft state machine |
| `internal/wal` | Append-only file, checksum, `Sync`, replay | Raft log |
| Engine | WAL first, then apply, then ack | Leader apply path |
| `cmd/deepstore` | Process you can run and kill | Node binary |

Put/Delete never touch the map until the record is on disk. Get never goes to the WAL.

## Record layout

One record is one atomic mutation. Length-prefix so you know how many bytes to read; CRC so a torn tail is detectable.

```
[ crc32 ][ length ][ payload ]
   4B        4B       length bytes

payload:
[ op:1B ][ key_len:4B ][ key ][ val_len:4B ][ value ]
```

- `op`: `1` = Put, `2` = Delete. Delete still carries `val_len = 0`.
- `crc32`: IEEE CRC of `length || payload` (not including the CRC field).
- Integers: unsigned, little-endian.
- No record sequence number yet. File order is the total order.

Reject empty keys. Copy value bytes out of the read buffer so later reads cannot alias WAL pages.

## fsync policy

Phase 1: **`Sync()` every record before ack.** One `write` of the full framed record, then `file.Sync()`.

| Policy | Meaning |
| --- | --- |
| Acked | `write` + `Sync` both returned nil, then the map was updated, then the client got OK |
| In OS cache only | Invisible after crash. Must not ack |
| Group commit (later) | Batch several records, one `Sync`. Same rule: no ack until that `Sync` returns |

`write` success is not durability. `Sync` is. If `Sync` fails, do not apply, do not ack, treat the process as unsafe until you inspect the file.

Directory `Sync` after creating the WAL file (create + parent dir fsync) so a crash cannot lose the directory entry.

## Replay

On open:

1. Create or open `wal.log` in the data dir.
2. Read records from offset 0.
3. Valid CRC + length: apply to the empty map (Put or Delete).
4. Short read at EOF, or a CRC mismatch with no valid record after it: torn tail. Truncate back to the last good offset. Stop. A full length with a bad CRC is still a torn tail when the file was extended before the payload landed.
5. Bad CRC followed by a valid record: refuse to start. Do not skip. Corruption is not a torn tail.
6. Seek to the last good offset and append from there.

Replay is the only writer of the map at startup. Live Puts after open take the same path: frame → write → Sync → apply → ack.

## What "acked" means

A client-visible OK is a durability and linearizability claim:

- Restart after ack: Get must return that value (or a later acked value for the same key).
- Crash before ack: Get may return the old value. The client must retry.
- Two concurrent Puts on one key: the mutex around "WAL then apply" gives a single winner. The acked winner is the one whose record landed first.

That is single-node linearizability. The cluster later has to preserve the same story across machines.

## ACID, honestly

- Atomicity: one record, all or nothing (CRC / torn tail).
- Consistency: keys exist or they do not. No invariants beyond that.
- Isolation: one-key linearizability via the lock, not SNAPSHOT/SSI.
- Durability: `Sync` then ack, then replay.

No multi-key transactions, no LSM, no Raft, no gRPC in this phase.

## Package sketch (you write the code)

```
internal/store   Mem. Get / Put / Delete / Apply(record). Mutex or RWMutex.
internal/wal     Open, Append+Sync, Replay(func), Close. No KV logic.
internal/engine  Wires WAL then store. This is what the binary calls.
cmd/deepstore    After crash-replay tests pass. Tiny CLI or HTTP.
```

Store tests: table-driven Get/Put/Delete; concurrent Puts on one key have one final value; Get does not race with Put.

WAL/engine tests: Put, close, reopen, Get; truncate the last N bytes of `wal.log` to simulate a torn write, reopen, confirm last *acked* Put survived and the torn one did not; if you ack, a following process-restart Get must see it.

Binary: data-dir flag, put/get/delete, then `kill -9` and start again by hand.
