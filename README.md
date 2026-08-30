# History

History by PLAYCITY BLOCK

History records world changes on Paper and provides scoped inspection, rollback, and undo tools. It is implemented independently and does not depend on CoreProtect.

This alpha supports SQLite and PostgreSQL. It can capture compatible WorldEdit and FastAsyncWorldEdit sessions.

## Requirements

- Paper 26.2
- Java 25
- WorldEdit or FastAsyncWorldEdit is optional

## Installation

1. Build or download `History-0.4.0-alpha.7.jar`.
2. Copy it into the server's `plugins` directory.
3. Restart Paper normally. Do not hot-reload the plugin.
4. Run `/history status` and confirm that storage is ready.

Back up the world and plugin data before evaluating an alpha build on an existing server.

## Usage

Run `/history` to open the guided controls. Detailed lookup is also available to players and the server console.

```text
/history lookup u:Builder t:7d r:25 a:worldedit i:stone,!dirt limit:20
/history lookup w:world x:100 z:-40 t:1d r:10
/history lookup w:world x:100 y:64 z:-40 t:30d r:0
/history status
/history storage
/history recover 00000000-0000-0000-0000-000000000000
```

Lookup keys are `u/user`, `t/time`, `r/radius`, `a/action`, `i/item`, `e/exclude`, `w/world`, `x`, `y`, `z`, and `limit`. Console lookup requires a loaded world and X/Z coordinates.

If `/history status` reports a capture gap, History keeps later capture attempts running and shows database backlog separately from gaps. Resolve any storage error, wait for pending work to drain, then run `/history resume`. This verifies the store and clears the active degraded state. Earlier gap counters remain visible.

If an operation remains `PREPARED` after an interruption, run `/history recover <operation-id>`. History streams the remaining database plan, checks each exact block, and does not reapply a block that already reached its target state.

## Safety

- Rollback uses a preview and explicit confirmation.
- The operation plan is stored before the first world change.
- Each exact chunk is checkpointed before the next chunk starts.
- Interrupted `PREPARED` operations can reconcile applied-but-uncheckpointed blocks by operation ID.
- Every operation is audited and can be reversed.
- Only chunks containing selected records may be loaded.
- Each target is checked again before it is changed.
- Recovery does not call WorldEdit or FAWE undo APIs.
- Database work runs outside the server thread.

## Current limits

- Sign and inventory payloads are recorded on Bukkit capture paths. Other block entities are marked explicitly as unsupported. Block-entity restoration is disabled and `restore-block-entity-data: true` is rejected to prevent partial updates, item loss, or duplication.
- FAWE batch capture records block states. FAWE block-entity NBT, biome changes, and entity changes are not presented as restorable history.
- Entity events are audit records only. Entity restoration, biome restoration, and CoreProtect import are not implemented.
- Folia is not supported.
- Rollback planning has no configured total record or chunk cap. It streams matching history to disk and restores one exact chunk at a time.

See [storage guidance](docs/STORAGE.md) for backend selection and capacity planning. The [independent implementation record](docs/INDEPENDENT_IMPLEMENTATION.md) describes the integration boundary.

## Build

```powershell
.\gradlew.bat clean build --console=plain
```

The distributable JAR is written to `build/libs/`.

## License

History is licensed under the [MIT License](LICENSE). Bundled and optional dependency notices are listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
