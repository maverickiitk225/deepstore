# Snapshot

`snapshot` in the data directory is the map and the session table at one applied index. After it is durable, log compaction drops every `wal.log` record at or below that index. Open installs the snapshot and replays only the tail.

Record layout, `Sync`, and torn tails are in [durability.md](durability.md). Sessions and the three indexes are in [node.md](node.md). `serve -snapshot-every` is in [cmd/README.md](../cmd/README.md).

## When

The engine writes a snapshot after that many records have been applied since the previous one. The default is 1024. `Snapshot` writes one immediately. `0` disables the automatic ones.

The writer does this on its own goroutine, after the batch that crossed the threshold has been acknowledged. The copy is the map and the session table at `appliedIndex`. Queued commands wait until the snapshot and the compaction finish. They are not included in that snapshot. An engine whose applied index is still 0 writes no file.

A failed snapshot does not undo acknowledged writes. Those records stay in the log. `snapshotIndex` does not advance, and the next attempt writes the file again. The automatic path tries on the next batch. `Snapshot` returns the error to its caller.

## File

The file is one checksummed payload. Integers are unsigned and little-endian. The CRC is the IEEE CRC of `length || payload`, the same function as the log.

```
[ crc32:4B ][ length:4B ][ payload ]

payload v1:
[ version:1B ][ index:8B ][ nkeys:4B ]
[ key_len:4B ][ key ][ val_len:4B ][ value ]  // nkeys times, key bytes ascending
[ nsess:4B ]
[ client_id:8B ][ seq:8B ][ index:8B ][ swapped:1B ]  // nsess times, client id ascending
```

| Field | Rule |
| --- | --- |
| `version` | `1`. Any other version fails open. |
| `index` | The applied index included in the snapshot. Records at or below it are not replayed. |
| keys | Non-empty. An empty value is stored. A missing key is absent. Key bytes ascend. |
| sessions | Client id and sequence are non-zero. The session index is between 1 and the snapshot index. `swapped` is 0 or 1. Client ids ascend. |
| payload length | Above 1 GiB fails open. |

Keys and client ids are sorted so the same state encodes to the same bytes. Key and value bytes are copied out of the read buffer on decode.

The bytes go to `snapshot.tmp`, the file is synced, then renamed to `snapshot`, and the directory is synced. A crash before the rename leaves the previous `snapshot`. The next `Save` truncates a leftover `snapshot.tmp`. A short file, a bad checksum, an unknown version, or a length past the maximum is corruption, and open fails. There is no torn-tail truncation: the installed file is one payload, published only after its sync.

## Compaction

Compaction rewrites `wal.log` so it contains only records with an index above the snapshot. The kept tail is copied into `wal.log.tmp`, that file is synced, the live log is closed, and `wal.log.tmp` is renamed over `wal.log`. The directory is synced, and the new file is opened at its end. `lastIndex` does not change, so the next append is still the last index plus one.

A log that already begins at the snapshot index plus one is left in place. A log whose every record is at or below the snapshot becomes an empty file. The following append still uses `lastIndex + 1`. A later snapshot compacts a log that no longer starts at 1. The first record then has to be at most the new snapshot index plus one, and each record still has to be the previous index plus one.

`index` 0 leaves the file as it is. Open deletes a leftover `wal.log.tmp`. That file is the unpublished rewrite. `wal.log` is still the log.

Compaction uses its own sync of the new file and of the directory. It does not call the batch `Sync`.

If the new log cannot be opened after the old file is closed, the engine is poisoned. Later writes fail until the process exits. The snapshot is already durable, and the next open recovers from it. Any other compaction error leaves the engine writable. The snapshot file is the new one, and the next snapshot tries the drop again.

## Open

Open loads `snapshot` when the file exists and restores the map and the session table. It then reads `wal.log`. Records at or below the snapshot index are checked and not applied. Open then compacts, dropping that prefix if it is still in the file. A crash between the snapshot and the compaction is repaired here.

With no snapshot, replay starts from an empty map and the log is not compacted.

| Log | Result |
| --- | --- |
| No `snapshot`, log starts at 1 | Replay the whole log. |
| Empty log, snapshot at `S` | `lastIndex` is `S`. The next record is `S + 1`. |
| Log starts at 1 and runs through `S` or past it | Check every record. Apply only indexes above `S`. Then drop records at or below `S`. |
| Log starts at `S + 1` | The prefix is already gone. Apply the tail. Compaction leaves the file in place. |
| Every decoded record is below `S` | The snapshot covers them. Truncate the file to empty. `lastIndex` is `S`. |
| First index is neither 1 nor `S + 1` | Open fails. |
| Gap, repeated index, or a suffix that starts past `S + 1` | Open fails. |
| Short `snapshot`, bad checksum, unknown version, or length above 1 GiB | Open fails. |

A torn tail of the log is still truncated to the last good record, as in [durability.md](durability.md). If that leaves the log ending below `S`, the remaining bytes are covered by the snapshot and the file is truncated to empty.

## Crash

The snapshot is published before any byte of the prefix is dropped. Either file the next open finds is enough to rebuild the acknowledged state.

| Crash | On disk | Next open |
| --- | --- | --- |
| During `snapshot.tmp`, before the rename | Previous `snapshot`, or none, and the full log | Replay from the previous snapshot, or from an empty map. A leftover `snapshot.tmp` is ignored. |
| After `snapshot` is renamed and the directory is synced, before the log rewrite | New `snapshot`, full log | Install the snapshot, skip records at or below its index, then compact. |
| During `wal.log.tmp`, before the rename | New `snapshot`, old `wal.log` | Delete `wal.log.tmp`. Same recovery as the row above. |
| After `wal.log` is renamed | New `snapshot`, log is the tail or empty | Replay the tail onto the snapshot. Compaction finds the prefix already gone. |

A crash before the response still leaves a command either in the log or absent. The snapshot does not include a command that failed `Sync`, because that command was not applied. The client retries with the same sequence, described in [node.md](node.md).

## Indexes

`snapshotIndex` is the index stored in `snapshot`. Just after a snapshot it equals `lastIndex` and `appliedIndex`. Records appended after that make those two greater, and the next open replays that tail. `lastIndex` stays the high watermark through compaction, including when the rewritten file is empty.

## Tests

```
go test ./internal/snapshot ./internal/wal ./internal/engine
```

| Coverage | Test |
| --- | --- |
| Round trip, byte-stable encoding, torn file, bad CRC, unknown version, session past the snapshot | `internal/snapshot` |
| Replay skips a covered prefix; a log that ends below the snapshot truncates | `TestWALOpenAfterSkipsCoveredPrefix`, `TestWALOpenAfterEmptyKeepsBase` |
| A suffix that starts past the snapshot index plus one fails open | `TestWALOpenAfterRejectsGap` |
| Dropping a prefix keeps the tail, including a second drop over a compacted log | `TestWALDiscardThroughKeepsTail`, `TestWALDiscardThroughTwice` |
| Snapshot, reopen, an empty value, a deleted key, and a tail record | `TestEngineSnapshotReopen` |
| A compare-and-swap result survives the snapshot and the empty log | `TestEngineSnapshotSessionAndCAS` |
| A snapshot installed before the prefix is dropped, and one that already covers the log | `TestEngineOpenAppliesTailPastSnapshot`, `TestEngineOpenSkipsPrefixCoveredBySnapshot` |
| Automatic snapshots, including a second one over a compacted log | `TestEngineSnapshotEvery` |
| A corrupt `snapshot` fails open while `wal.log` is still valid | `TestEngineCorruptSnapshotFailsOpen` |
