# Storage

SQLite is the default backend. PostgreSQL is optional. PostgreSQL moves storage out of the Minecraft process, but it does not automatically use less disk space.

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

Direct changes and FAWE changes use separate bounded queues, so a large edit cannot consume the headroom reserved for players and server events. `storage.queue-capacity` applies to each lane.

FAWE workers apply backpressure when their queue reaches its bound. They resume as the database writer drains records instead of dropping history. Direct server-thread capture never waits for a database round trip; a direct-queue overflow or storage failure remains visible through `/history status` rather than being reported as persisted.

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

Monitor accepted, persisted, merged, rejected, queued, and purged counts. PostgreSQL operators should also monitor commit latency, WAL generation, and autovacuum lag.
