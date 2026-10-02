# deepstore

Educational project: a practical, linearizable key-value store. The end goal is a small distributed system. The current work is a **single-node durable engine** (in-memory map + WAL) behind a long-running HTTP server. That is not a SQL database and not yet a cluster.

See [docs/phase1-engine.md](docs/phase1-engine.md) for the record layout, fsync policy, and what an ack means. See [docs/phase2-node.md](docs/phase2-node.md) for the node shape this is growing into. The binary and HTTP routes are in [cmd/README.md](cmd/README.md).

## Properties

These are what the single node guarantees today.

**Acked writes are durable.** Put and Delete append one record, `fsync`, apply it to the map, then return the log index. After a restart, Get returns that value, or a later acked value for the same key. A write that never acked is not a promise: after a crash it may be there or not.

**A record is atomic.** The frame is CRC32, length, then a versioned payload. On open, a torn tail (a short read at EOF, or a bad CRC with no valid record after it) is truncated to the last good record. A bad CRC followed by a valid record, an index gap or repeat, an unknown version, or a length past the maximum is corruption, and open fails.

**A failed sync poisons the process.** The map is left unchanged and the caller gets an error. Later Put and Delete calls fail until restart. Restart replays the bytes that are actually in the file.

**One key is linearizable.** The commit lock orders append, sync, and apply, so concurrent writes to one key have a single order. Get reads the map. Concurrent Put/Get histories are checked with [Porcupine](https://github.com/anishathalye/porcupine), including runs that kill the server and start it again on the same data directory. A Put that timed out or lost its connection is an unknown outcome: it may have been synced or not.

**The log is a total order.** Indices start at 1 and increase by one. After every ack, the last index on disk and the last index applied to the map are the same. Open restores both from the WAL.

**Close does not drop an ack.** Close takes the commit lock, so an in-flight write either returns success and is on disk, or returns an error and is not applied. Writes after close fail. SIGINT and SIGTERM stop accepting, finish in-flight requests, then close the WAL. Durability does not depend on that path: the crash test kills the process and checks the history anyway.

**A new WAL file survives as a directory entry.** Creating `wal.log` is followed by an fsync of the parent directory.

Group commit, client sessions, conditional writes, and snapshots are still ahead.
