# Storage

SQLite is the default backend. PostgreSQL is optional. PostgreSQL moves storage out of the Minecraft process, but it does not automatically use less disk space.

Capture first drains to `capture-journal.wal` on a dedicated worker. Database projection and history reads use separate connections and workers. Long lookups, rollback scans, a slow remote database, and a database connection that is unavailable during startup therefore do not stop the capture journal from draining the bounded memory queues. Failed writer and reader startup connections retry with bounded exponential backoff while capture remains available. Each read first persists captures accepted before that read began, but it does not wait for newer continuous traffic to stop. SQLite uses WAL for concurrent readers; PostgreSQL uses an independent read connection.

The local capture journal uses length-delimited CRC32C frames and a forced checkpoint. An acknowledged empty journal is compacted back to its header. On restart, complete unacknowledged frames are replayed before History becomes ready. Every new capture has a durable UUID, so a database commit followed by a crash before journal acknowledgement is replayed idempotently instead of duplicated. A corrupt complete frame stops History rather than being skipped. An incomplete final crash frame is removed and reported as an unknown-size capture gap.

## Data reduction

History reduces repeated data before it is stored:

- worlds, actors, edit batches, and block states use dictionary IDs;
- positions and rollback source IDs use compact binary encodings;
- large block-entity payloads are compressed only when useful;
- container changes in one tick are coalesced per block;
- continuous changes at one coordinate in the same WorldEdit or FAWE batch are merged safely;
- chat logging is disabled by default.

`/history storage` reports physical storage use and estimated input grouped by cause. Shared dictionaries and compression prevent exact physical-byte attribution per cause.

## Backpressure

Direct changes and FAWE changes use separate bounded queues, so a large edit cannot consume the headroom reserved for players and server events. `storage.queue-capacity` bounds direct capture. `storage.worldedit-queue-capacity` independently bounds WorldEdit and FAWE capture.

History observes FAWE only in post-processing. It never holds queue permits across unmatched FAWE callbacks and never throws a History capacity error into `set`, `cut`, `paste`, `stack`, `move`, `regen`, `undo`, or `redo`. One applied chunk is admitted to the bounded queue as an all-or-none batch when it fits. A FAWE worker waits for queue headroom up to `worldedit-admission-timeout-ms`; batches larger than the internal queue are streamed through bounded sub-batches. Server-thread fallback capture never waits. If storage still cannot admit a batch, the edit remains intact and History records a visible capture gap.

The WorldEdit fallback delegates the mutation first and then performs a non-blocking capture attempt. A History failure is not propagated through the edit path.

Direct server-thread capture never waits for filesystem or database I/O. Under sustained pressure, History drains memory to the sequential journal independently of database projection and expands database batches up to 8,192 records. If the in-memory ingress lane itself still overflows before the journal worker can drain it, that record becomes a visible capture gap while later changes continue to be attempted. After the queue drains and storage is healthy, `/history resume` verifies the store and clears the active degraded state without erasing cumulative gap counters.

Rollback queries do not use a total row or chunk cap. Matching rows are read with a bounded database fetch size, consolidated one block position at a time, and written to a verified temporary plan. The durable operation is then prepared in bounded batches and applied one exact chunk at a time. History checkpoints that chunk before loading the next one. If execution stops after world application but before a checkpoint is acknowledged, `/history recover <operation-id>` rebuilds the pending spool from the database and reconciles live `before`/`after` state. This keeps memory proportional to a database page and one target chunk instead of the total rollback area.

Place PostgreSQL on a reliable, low-latency network. Do not use a distant public-network database only to save local disk.

## Retention

`storage.retention.days: 0` keeps all records. A positive value deletes expired records in bounded batches.

SQLite uses incremental vacuum after retention work. PostgreSQL normally keeps freed pages for reuse. The plugin does not run `VACUUM FULL`.

## Backend selection

| Situation | Recommended backend |
|---|---|
| One Paper server and simple backups | SQLite |
| Shared storage, managed backups, or remote administration | PostgreSQL |
| High-latency or unreliable database network | Move the database closer first |

To use PostgreSQL, set `storage.backend: postgresql`. Prefer `password-env` over a plaintext password and configure TLS verification for remote connections.

History does not automatically import CoreProtect data or transfer records between SQLite and PostgreSQL. Select the intended backend before production use.

Monitor accepted, persisted, merged, direct rejects, capture-gap events/changes, volatile memory queue, durable journal/DB backlog and bytes, pending external edits, interrupted operations, and purged counts. PostgreSQL operators should also monitor commit latency, server WAL generation, and autovacuum lag.

The Paper event path remains non-blocking. A forced process or host failure can therefore still lose the very small tail accepted into memory but not yet forced by the journal worker. Eliminating that final window would require synchronous disk I/O on the server thread or rejecting/cancelling world changes, both of which violate History's availability boundary. Everything already forced to the local journal survives database outages and normal or abnormal restarts.
