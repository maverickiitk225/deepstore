# Phase 2: a single node that is shaped like a Raft node

Phase 1 proved one thing: an acked Put survives `kill -9`. Phase 2 does **not** add consensus. It turns the engine into a long-running server whose log, snapshots, client protocol and test harness already have the shape Raft needs. Phase 3 then swaps "append locally" for "replicate to a majority" without redesigning everything else.

Exit criterion: a linearizability checker says "OK" on a history recorded from many concurrent clients hammering the server while it is being `kill -9`ed and restarted.

## Why not jump to Raft now

Raft papers assume these already exist. If they do not, you end up debugging five things at once.

| Raft needs | Phase 1 has | Phase 2 adds |
| --- | --- | --- |
| Log entries addressed by `index` | File order only | Monotonic `index` in every record |
| `lastApplied` / `commitIndex` | Implicit | Explicit, persisted via snapshot |
| Snapshots + log truncation (`InstallSnapshot`) | WAL grows forever | Snapshot file + segmented WAL, delete old segments |
| Clients that retry safely (Raft §8) | CLI, one shot | Client ID + sequence number, dedup table in state machine |
| A process that stays up and serves RPCs | Opens WAL per command | Server with a network API |
| High write throughput despite `fsync` | One `fsync` per Put | Group commit |
| A way to know you are correct | Unit tests | Linearizability checker (Porcupine) |

## 1. Long-running server

The CLI currently opens the WAL, does one op, exits. A Raft node is a process that stays up.

- `cmd/deepstore serve -data-dir DIR -listen :7000`
- Transport: pick one and stick with it. Plain HTTP+JSON is easiest to debug with `curl`. gRPC is what etcd/CockroachDB/TiKV use and you will want it for Raft RPCs in Phase 3. Either is fine; do not build both.
- Keep the existing `put/get/delete` subcommands as a **client** that talks to the server.
- Graceful shutdown on SIGTERM (stop accepting, drain, close WAL). `kill -9` must still be safe; graceful shutdown is an optimisation, not a correctness requirement.

## 2. Log index

Every record gets a `uint64 index`, starting at 1, strictly increasing, no gaps.

```
payload (v3):
[ version:1B ][ index:8B ][ client_id:8B ][ seq:8B ][ op:1B ][ key_len:4B ][ key ][ val_len:4B ][ value ]
[ exp_len:4B ][ expected ]   // CAS only
```

Version 2 records have no `client_id` or `seq` and still replay.

- Replay checks `index == previous + 1`. A gap or repeat is corruption, not a torn tail.
- The engine tracks `lastIndex` (last on disk) and `appliedIndex` (last applied to the map). In Phase 2 they are equal after every ack. In Phase 3 they diverge and `commitIndex` sits between them.
- Leave room for `term:8B` now or add it in Phase 3 behind the version byte. Think about which one you would rather migrate.
- Put a format version somewhere (per record or a file header). You are about to change the format for the second time.

## 3. Client sessions and exactly-once

A client sends Put, the server applies it, the response is lost, the client retries. Without dedup the Put is applied twice. For a plain Put that is harmless; for anything conditional (CAS, increment, delete-then-put) it breaks linearizability.

- Each client has a `clientID` and a monotonic `seq`.
- The record carries `(clientID, seq)`. The **state machine** (not the server layer) keeps `lastSeq[clientID]` and the cached response.
- On apply: `seq <= lastSeq` → return cached result, do not mutate.
- The dedup table is part of state, so it must be in snapshots and rebuilt on replay. This is exactly the Raft dissertation §6.3 design.
- Add one conditional op to prove it matters: `CAS(key, expected, new)`.

## 4. Group commit

`fsync` per Put caps you at roughly 1/fsync-latency writes per second (often a few hundred on a laptop SSD with `F_FULLFSYNC`, far less on spinning disks).

- Many goroutines submit records to a single writer goroutine via a channel.
- The writer drains whatever is queued, does one `write` for the batch, one `Sync`, then applies all of them in order and wakes every waiter.
- Same rule as Phase 1: nobody gets an ack until the `Sync` covering their record returns.
- Measure before and after with a small load generator (N concurrent clients, report ops/s and p50/p99 latency).
- macOS note: Go's `File.Sync` issues `F_FULLFSYNC` on darwin. Know which one you are benchmarking.

This single-writer loop is the same shape as a Raft leader's "append then replicate then commit" loop.

## 5. Snapshots and log compaction

The WAL grows forever and replay time grows with it.

- **Segmented WAL:** `wal-<firstIndex>.log`, roll to a new segment at a size threshold. Deleting old history becomes "unlink whole files", never rewriting a live file.
- **Snapshot:** serialize the map + dedup table + `lastIncludedIndex` to `snap-<index>.tmp`, `fsync`, `rename` to `snap-<index>`, `fsync` the directory. Temp-write-fsync-rename-fsync-dir is the only crash-safe way to publish a file.
- **Recovery:** load newest valid snapshot, then replay WAL records with `index > lastIncludedIndex`. If the newest snapshot is corrupt, fall back to the previous one (so do not delete it too eagerly).
- **Compaction:** after a snapshot is durable, delete segments whose records are all `<= lastIncludedIndex`.
- **Concurrency:** taking a snapshot must not stop writes for its whole duration. Options: copy the map under the lock (simple, memory x2), or copy-on-write / persistent map. Start simple.

In Phase 3 the same snapshot file is what a leader ships to a lagging follower via `InstallSnapshot`.

## 6. Linearizability testing

Unit tests prove the pieces. They do not prove the system is linearizable under concurrency and crashes.

- Write a test harness that runs N concurrent clients doing random Put/Get/CAS on a small key set, recording `(clientID, op, input, output, invokeTime, returnTime)` for each call.
- Randomly `kill -9` and restart the server during the run. Ops whose outcome is unknown (timeout / connection reset) are recorded as "may or may not have happened".
- Feed the history to [Porcupine](https://github.com/anishathalye/porcupine) with a KV model. It either says linearizable or shows you a visualisation of the violating history.
- Keep this harness. In Phase 3 you point it at a 3-node cluster and add network partitions.

## What "acked" means now

Same as Phase 1, plus:

- An ack carries the log index the write landed at.
- A retried `(clientID, seq)` returns the original result, never applies twice.
- After restart the server returns identical state whether it recovered from WAL only or from snapshot + WAL tail.

## Out of scope

Raft, multiple nodes, leader election, sharding, LSM / SSTables, range scans, multi-key transactions, MVCC. Snapshots are a full dump of the map, not an LSM.

## Package sketch (you write the code)

```
internal/wal       Segments, index in records, version byte, group-commit writer.
internal/snapshot  Write (tmp+fsync+rename+dirfsync), Load newest valid, List/Delete old.
internal/store     Map + dedup table. Apply returns a result. Snapshot()/Restore().
internal/engine    Owns lastIndex/appliedIndex, snapshot trigger, compaction.
internal/server    Network API. Translates requests to engine calls.
cmd/deepstore      `serve` subcommand + client subcommands.
test/linearizable  Load generator + history recorder + Porcupine check + crash injector.
```

## Suggested order

1. Log index + format version. Replay tests for gaps/repeats.
2. Server + client subcommands.
3. Linearizability harness against the server (no crashes yet). Get it green early; it guards every later step.
4. Add crash injection to the harness.
5. Group commit. Harness must stay green; benchmark shows the win.
6. Client sessions + CAS. Harness with retries must stay green.
7. Segmented WAL, then snapshots, then compaction. Test recovery from: WAL only, snapshot only, snapshot + tail, corrupt newest snapshot.

## Tests to write

- Replay rejects index gap, repeated index, unknown version.
- Group commit: kill between `write` and `Sync` (simulate by not syncing) → no waiter in that batch was acked, none survive or all survive consistently.
- Dedup: same `(clientID, seq)` applied twice → one mutation, same response; survives restart and snapshot restore.
- Snapshot crash points: crash before rename, after rename before dir fsync, after snapshot before compaction. Each must recover to the same state.
- Porcupine: 10+ clients, 3-5 keys (small key space = more conflicts), several crashes per run, runs in CI.

## Resources

- Raft paper, ["In Search of an Understandable Consensus Algorithm"](https://raft.github.io/raft.pdf) — read §5 and §7 (log compaction) now, so Phase 2 choices line up.
- Ongaro's [Raft dissertation](https://github.com/ongardie/dissertation) — §5 (log compaction), §6.3 (client sessions / exactly-once), §6.4 (linearizable reads).
- [MIT 6.5840 labs](https://pdos.csail.mit.edu/6.824/) — Lab 3/4 use exactly this snapshot + dedup design, and the tests use Porcupine.
- [Porcupine](https://github.com/anishathalye/porcupine) and the blog post ["Testing Distributed Systems for Linearizability"](https://anishathalye.com/testing-distributed-systems-for-linearizability/).
- Herlihy & Wing, ["Linearizability: A Correctness Condition for Concurrent Objects"](https://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf) — the definition Porcupine checks.
- etcd `wal` and `snap` packages ([go.etcd.io/etcd/server/v3/storage/wal](https://github.com/etcd-io/etcd/tree/main/server/storage/wal)) — segmented WAL, CRC chaining, torn-tail handling, snapshot files. Read after you have your own version.
- Pillai et al., ["All File Systems Are Not Created Equal"](https://www.usenix.org/system/files/conference/osdi14/osdi14-paper-pillai.pdf) (OSDI '14) — why tmp+fsync+rename+dirfsync, and what breaks without it.
- ["Can Applications Recover from fsync Failures?"](https://www.usenix.org/system/files/atc20-rebello.pdf) (ATC '20) and the PostgreSQL fsyncgate discussion — why you poison on `Sync` error.
- Jepsen's [etcd](https://jepsen.io/analyses/etcd-3.4.3) and [consistency models](https://jepsen.io/consistency) pages — what a real linearizability analysis looks like.

## After Phase 2

Phase 3 is Raft: terms in the log, leader election, `AppendEntries`, commit index, followers applying from the log, `InstallSnapshot`, linearizable reads (ReadIndex or leader leases). The storage, snapshot, dedup and test harness from this phase are reused as-is.
