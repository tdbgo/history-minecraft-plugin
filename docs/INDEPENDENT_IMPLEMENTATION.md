# Independent implementation record

History is implemented independently in the `kr.playcity.history` namespace.
It does not link to CoreProtect, read or write CoreProtect tables, load
CoreProtect classes, copy CoreProtect source, or require CoreProtect at runtime.

## Independence boundary

- Runtime compatibility is implemented through the public Paper, Bukkit,
  WorldEdit, and FAWE APIs.
- History has its own data model, SQLite/PostgreSQL schemas, command interaction, migration
  sequence, operation journal, and tests.
- CoreProtect classes, source code, database tables, and data rows are not used
  by the runtime implementation.
- Importing historical data from another product is not part of the runtime
  implementation. Any future importer must be a separately reviewed, explicit,
  read-only-source migration tool.

## Build enforcement

The distribution verification task requires History's license and notices in
the JAR and fails if CoreProtect classes, Paper classes, WorldEdit classes, or
FAWE classes are bundled. It additionally rejects WorldEdit/FAWE references
and broad chunk-range/force-load APIs from the rollback package, enforcing that
recovery cannot call their undo/region mutation implementations or expand an
exact target set into a server-wide chunk operation.
