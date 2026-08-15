# History

History by PLAYCITY BLOCK

History records world changes on Paper and provides scoped inspection, rollback, and undo tools. It is implemented independently and does not depend on CoreProtect.

This alpha supports SQLite and PostgreSQL. It can capture compatible WorldEdit and FastAsyncWorldEdit sessions.

## Requirements

- Paper 26.2
- Java 25
- WorldEdit or FastAsyncWorldEdit is optional

## Installation

1. Build or download `History-0.4.0-alpha.3.jar`.
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
/history storage
```

Lookup keys are `u/user`, `t/time`, `r/radius`, `a/action`, `i/item`, `e/exclude`, `w/world`, `x`, `y`, `z`, and `limit`. Console lookup requires a loaded world and X/Z coordinates.

## Safety

- Rollback uses a preview and explicit confirmation.
- The operation plan is stored before the first world change.
- Every operation is audited and can be reversed.
- Only chunks containing selected records may be loaded.
- Each target is checked again before it is changed.
- Recovery does not call WorldEdit or FAWE undo APIs.
- Database work runs outside the server thread.

## Current limits

- Block-entity payloads are recorded, but restoring them is disabled by default to prevent item loss or duplication.
- Entity restoration and CoreProtect import are not implemented.
- Folia is not supported.
- Rollback size is bounded by the configured source-change and chunk limits.

See [storage guidance](docs/STORAGE.md) for backend selection and capacity planning. The [independent implementation record](docs/INDEPENDENT_IMPLEMENTATION.md) describes the integration boundary.

## Build

```powershell
.\gradlew.bat clean build --console=plain
```

The distributable JAR is written to `build/libs/`.

## License

History is licensed under the [MIT License](LICENSE). Bundled and optional dependency notices are listed in [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).
