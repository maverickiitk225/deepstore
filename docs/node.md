# Node

`deepstore serve` opens the engine and serves HTTP until SIGINT or SIGTERM. Shutdown stops accepting, finishes in-flight requests, and closes the WAL. Flags and routes are in [cmd/README.md](../cmd/README.md). Record layout, `Sync`, and replay are in [durability.md](durability.md). Snapshots and log compaction are in [snapshot.md](snapshot.md).

`put`, `get`, `delete`, and `cas` are clients of that server. `internal/client` uses one random non-zero client id per process. `NextSeq` is the next sequence. `PutSeq`, `DeleteSeq`, and `CASSeq` send a chosen sequence. The CLI sends sequence 1.

## Indexes

`lastIndex` is the last record in the log. `appliedIndex` is the last command applied to the map. They are equal after every successful write and after replay. `snapshotIndex` is the applied index stored in `snapshot`. Just after a snapshot the three match. A tail written after that leaves `snapshotIndex` behind the other two. Recovery is in [snapshot.md](snapshot.md).

The writer assigns the index before `AppendMany`. The first record is 1. Each new record is the previous index plus 1. Replay rejects a gap or a repeat.

## Sessions

Every engine write carries a non-zero `client_id` and `seq`. The state machine stores the latest sequence for that client and its result: the log index, and `swapped` for CAS.

The writer classifies the command before appending:

| `seq` | Effect |
| --- | --- |
| `lastSeq + 1` | Appended and applied. The first command for a client is sequence 1. |
| `lastSeq` | The stored result is returned. The log is unchanged. |
| Any other value | `errs.ErrSession`, HTTP 400. The log is unchanged. |

`Apply` uses the same table, so replay restores it. A repeated sequence in the log leaves the map unchanged.

A batch is classified against the stored table plus commands already accepted in that batch. Two new sequences from one client are appended in order. A second copy of a sequence in the batch receives the first copy's result. A batch made only of retries and rejected sequences is not written.

Version 2 records have no session and still replay. New writes are version 3.

## Compare-and-swap

`CAS(key, expected, value)` appends a record in either case.

| Current map | Result |
| --- | --- |
| Key present, value equals `expected` | Value replaced. `swapped` is true. |
| Key missing, or value differs | Map unchanged. `swapped` is false. The index advances. |

A retry of that sequence returns the original index and the original `swapped` value.

## Group commit

Callers enqueue a command and wait. One writer goroutine:

1. Classifies the queue against the session table.
2. Assigns indexes to the new commands.
3. Appends those records with one `write`.
4. Calls `Sync` once.
5. Applies them in index order.
6. Replies to every waiter. Retries receive the stored result.

A `Sync` error replies to the whole batch and poisons the engine. `AppendMany` may already have copied the bytes into the file. The next open replays them. Callers that saw the error retry with the same sequence.

## Linearizability

`test/linearizable` records each call as client, operation, input, output, call time, and return time. [Porcupine](https://github.com/anishathalye/porcupine) checks that history against a register model partitioned by key. A failed check writes an HTML file and the test prints the path.

`TestServerPutGetLinearizable` uses 8 clients, 20 operations each, and keys `a` through `e`.

`TestServerPutGetLinearizableUnderCrash` builds `./cmd/deepstore`, uses the same clients and keys, and `kill -9`s the process three times, restarting it on the same directory. `serve -snapshot-every 8` writes a snapshot and compacts the log during that run, so a kill can land in either step. The test fails if no `snapshot` file was published. A Put that times out or loses its connection is recorded as unknown and retried with the same sequence. The model accepts that write as either applied or absent. A Get that fails because the process is down is retried and left out of the history until it returns.

The history covers Put and Get. CAS is covered by the engine and server tests.

## Acknowledgement

A successful response includes the log index of the command that first ran that client sequence. A retry of the sequence returns the same index and, for CAS, the same `swapped` value. `lastIndex` and `appliedIndex` match the response, and they match again after replay.

## Tests

```
go test ./internal/wal ./internal/engine ./internal/server ./internal/client ./test/linearizable
go test -bench=BenchmarkPutConcurrent -benchtime=1s ./internal/engine
```

| Coverage | Test |
| --- | --- |
| Index increases by one and survives reopen; replay rejects a gap, a repeat, or an unknown version | `TestEngineIndexMonotonic`, `internal/wal` |
| Queued Puts share one `Sync`; a failed `Sync` acknowledges nobody | `TestEngineGroupCommitOneSync`, `TestEngineGroupCommitSyncFailureAcksNobody` |
| Throughput, p50, and p99 for 1, 4, 16, 64, and 256 writers | `BenchmarkPutConcurrent` |
| Retry, gap, old sequence, and retry after reopen | `TestApplySession`, `TestEngineSessionRetry`, `TestSessionRetryAndGap` |
| CAS retry after another client writes | `TestEngineSessionFailedCASStaysFailed`, `TestEngineSessionCASRetryAfterOtherWrite` |
| Two copies of one sequence in one batch | `TestEngineSessionDuplicateInBatch` |
| Version 2 replay, then a version 3 write | `TestEngineReplaysUnsessionedRecord` |
| HTTP Put, Get, Delete, and CAS | `internal/server` |
| Concurrent Put and Get | `TestServerPutGetLinearizable` |
| The same history across three `kill -9` restarts, with snapshots and log compaction | `TestServerPutGetLinearizableUnderCrash` |
