# deepstore

Educational project: a practical, linearizable key-value store. The end goal is a small distributed system. The current work is a **single-node durable engine** (in-memory map + WAL). That is not a SQL database and not yet a cluster.

Phase 1 durability: one Put/Delete is one log record, `fsync` before ack, replay on restart. Isolation is one-key linearizability, not SNAPSHOT or SSI.

See [docs/phase1-engine.md](docs/phase1-engine.md) for the record layout, fsync policy, and what an ack means.

Phase 2 turns the engine into a long-running server with log indices, group commit, client sessions, snapshots and a linearizability test harness. See [docs/phase2-node.md](docs/phase2-node.md).
