package kr.playcity.history.storage;

import kr.playcity.history.model.ActorKind;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.AppliedOperationItem;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.OperationCompletion;
import kr.playcity.history.model.OperationCheckpoint;
import kr.playcity.history.model.OperationDraft;
import kr.playcity.history.model.OperationFinalization;
import kr.playcity.history.model.OperationHeader;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;
import kr.playcity.history.model.OperationStatus;
import kr.playcity.history.model.OperationSummary;
import kr.playcity.history.model.StoredOperation;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

final class SqliteHistoryRepository implements HistoryRepository {
    private static final int SCHEMA_VERSION = 4;
    private static final int STATE_CACHE_SIZE = 16_384;
    private static final int ACTOR_CACHE_SIZE = 4_096;
    private static final long OPTIMIZE_INTERVAL = 100_000L;
    private static final int LATEST_POSITION_BATCH_SIZE = 5_000;

    private final Path databaseFile;
    private final int busyTimeoutMillis;
    private final Map<UUID, Long> worldIds = lruCache(256);
    private final Map<UUID, Long> batchIds = lruCache(4_096);
    private final Map<ActorRef, Long> actorIds = lruCache(ACTOR_CACHE_SIZE);
    private final Map<BlockSnapshot, Long> stateIds = lruCache(STATE_CACHE_SIZE);
    private Connection connection;
    private long changesSinceOptimize;

    SqliteHistoryRepository(Path databaseFile, int busyTimeoutMillis) {
        this.databaseFile = databaseFile;
        this.busyTimeoutMillis = busyTimeoutMillis;
    }

    @Override
    public void open() {
        try {
            Path parent = databaseFile.toAbsolutePath().normalize().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:" + databaseFile.toAbsolutePath());
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA foreign_keys=ON");
                statement.execute("PRAGMA busy_timeout=" + busyTimeoutMillis);
                statement.execute("PRAGMA page_size=8192");
                statement.execute("PRAGMA auto_vacuum=INCREMENTAL");
                statement.execute("PRAGMA journal_mode=WAL");
                statement.execute("PRAGMA synchronous=NORMAL");
                statement.execute("PRAGMA temp_store=MEMORY");
                statement.execute("PRAGMA cache_size=-32768");
                statement.execute("PRAGMA mmap_size=268435456");
                statement.execute("PRAGMA wal_autocheckpoint=4096");
            }
            migrate();
        } catch (ClassNotFoundException | java.io.IOException | SQLException exception) {
            closeQuietly();
            throw new StorageException("Unable to open the History database", exception);
        } catch (RuntimeException exception) {
            closeQuietly();
            throw exception;
        }
    }

    private void migrate() throws SQLException {
        int version;
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("PRAGMA user_version")) {
            version = result.next() ? result.getInt(1) : 0;
        }
        if (version > SCHEMA_VERSION) {
            throw new StorageException(
                "Database schema " + version + " is newer than supported schema " + SCHEMA_VERSION
            );
        }
        if (version == SCHEMA_VERSION) {
            try {
                createStorageMetricsSchema();
                dropUnusedIndexes();
            } catch (SQLException exception) {
                throw new StorageException("History schema is incomplete at version " + version, exception);
            }
            return;
        }

        inTransaction(() -> {
            if (version == 0) {
                createSchemaV2();
            } else if (version == 1) {
                migrateV1ToV2();
            } else if (version == 2) {
                migrateV2ToV3();
            } else if (version != 3) {
                throw new StorageException("No migration path exists from schema " + version);
            }
            createStorageMetricsSchema();
            dropUnusedIndexes();
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("PRAGMA user_version=" + SCHEMA_VERSION);
            }
        });
        try (Statement statement = connection.createStatement()) {
            statement.execute("PRAGMA incremental_vacuum(1024)");
            statement.execute("PRAGMA optimize");
        }
        clearCaches();
    }

    private void createSchemaV2() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE worlds (
                    id INTEGER PRIMARY KEY,
                    uuid BLOB NOT NULL UNIQUE CHECK(length(uuid) = 16)
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE actors (
                    id INTEGER PRIMARY KEY,
                    uuid BLOB CHECK(uuid IS NULL OR length(uuid) = 16),
                    name TEXT NOT NULL,
                    kind INTEGER NOT NULL
                )
                """);
            statement.executeUpdate("""
                CREATE UNIQUE INDEX actors_uuid_kind
                    ON actors(uuid, kind) WHERE uuid IS NOT NULL
                """);
            statement.executeUpdate("""
                CREATE UNIQUE INDEX actors_name_kind
                    ON actors(name COLLATE NOCASE, kind) WHERE uuid IS NULL
                """);
            statement.executeUpdate("""
                CREATE TABLE block_states (
                    id INTEGER PRIMARY KEY,
                    fingerprint BLOB NOT NULL UNIQUE CHECK(length(fingerprint) = 32),
                    block_data TEXT NOT NULL,
                    payload_type TEXT,
                    payload BLOB,
                    reference_count INTEGER NOT NULL DEFAULT 0 CHECK(reference_count >= 0)
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE edit_batches (
                    id INTEGER PRIMARY KEY,
                    uuid BLOB NOT NULL UNIQUE CHECK(length(uuid) = 16)
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at INTEGER NOT NULL,
                    world_id INTEGER NOT NULL,
                    chunk_x INTEGER NOT NULL,
                    chunk_z INTEGER NOT NULL,
                    packed_position INTEGER NOT NULL,
                    actor_id INTEGER NOT NULL,
                    cause INTEGER NOT NULL,
                    before_state_id INTEGER NOT NULL,
                    after_state_id INTEGER NOT NULL,
                    operation_uuid BLOB CHECK(operation_uuid IS NULL OR length(operation_uuid) = 16),
                    batch_id INTEGER,
                    metadata TEXT,
                    FOREIGN KEY (world_id) REFERENCES worlds(id),
                    FOREIGN KEY (actor_id) REFERENCES actors(id),
                    FOREIGN KEY (before_state_id) REFERENCES block_states(id),
                    FOREIGN KEY (after_state_id) REFERENCES block_states(id),
                    FOREIGN KEY (batch_id) REFERENCES edit_batches(id)
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX changes_world_chunk_time
                    ON changes(world_id, chunk_x, chunk_z, occurred_at DESC, id DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX changes_world_position_time
                    ON changes(world_id, chunk_x, chunk_z, packed_position, occurred_at DESC, id DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX changes_actor_time
                    ON changes(actor_id, occurred_at DESC, id DESC)
                """);
            statement.executeUpdate("""
                CREATE TABLE operations (
                    operation_uuid BLOB PRIMARY KEY CHECK(length(operation_uuid) = 16),
                    created_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    actor_id INTEGER NOT NULL,
                    kind INTEGER NOT NULL,
                    status INTEGER NOT NULL,
                    summary TEXT NOT NULL,
                    inverse_of BLOB CHECK(inverse_of IS NULL OR length(inverse_of) = 16),
                    item_count INTEGER NOT NULL,
                    applied_count INTEGER NOT NULL DEFAULT 0,
                    skipped_count INTEGER NOT NULL DEFAULT 0,
                    failure TEXT,
                    FOREIGN KEY (actor_id) REFERENCES actors(id)
                ) WITHOUT ROWID
                """);
            statement.executeUpdate("""
                CREATE INDEX operations_actor_time
                    ON operations(actor_id, created_at DESC)
                """);
            statement.executeUpdate("""
                CREATE TABLE operation_items (
                    operation_uuid BLOB NOT NULL CHECK(length(operation_uuid) = 16),
                    sequence INTEGER NOT NULL,
                    world_id INTEGER NOT NULL,
                    chunk_x INTEGER NOT NULL,
                    chunk_z INTEGER NOT NULL,
                    packed_position INTEGER NOT NULL,
                    before_state_id INTEGER NOT NULL,
                    after_state_id INTEGER NOT NULL,
                    source_ids BLOB,
                    applied INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (operation_uuid, sequence),
                    FOREIGN KEY (operation_uuid) REFERENCES operations(operation_uuid) ON DELETE CASCADE,
                    FOREIGN KEY (world_id) REFERENCES worlds(id),
                    FOREIGN KEY (before_state_id) REFERENCES block_states(id),
                    FOREIGN KEY (after_state_id) REFERENCES block_states(id)
                ) WITHOUT ROWID
                """);
        }
    }

    private void createStorageMetricsSchema() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS storage_metrics (
                    cause INTEGER PRIMARY KEY,
                    change_count INTEGER NOT NULL CHECK(change_count >= 0),
                    estimated_input_bytes INTEGER NOT NULL CHECK(estimated_input_bytes >= 0),
                    first_occurred_at INTEGER NOT NULL,
                    last_occurred_at INTEGER NOT NULL
                ) WITHOUT ROWID
                """);
        }
    }

    private void dropUnusedIndexes() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP INDEX IF EXISTS changes_operation");
            statement.executeUpdate("DROP INDEX IF EXISTS changes_batch");
        }
    }

    private void migrateV1ToV2() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP INDEX IF EXISTS changes_world_chunk_time");
            statement.executeUpdate("DROP INDEX IF EXISTS changes_world_position_time");
            statement.executeUpdate("DROP INDEX IF EXISTS changes_actor_time");
            statement.executeUpdate("DROP INDEX IF EXISTS changes_actor_name_time");
            statement.executeUpdate("DROP INDEX IF EXISTS changes_operation");
            statement.executeUpdate("DROP INDEX IF EXISTS operations_actor_time");
            statement.executeUpdate("ALTER TABLE changes RENAME TO changes_v1");
            statement.executeUpdate("ALTER TABLE operations RENAME TO operations_v1");
            statement.executeUpdate("ALTER TABLE operation_items RENAME TO operation_items_v1");
        }
        createSchemaV2();
        clearCaches();

        migrateV1Changes();
        migrateV1Operations();
        migrateV1OperationItems();
        rebuildStateReferenceCounts();

        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE operation_items_v1");
            statement.executeUpdate("DROP TABLE operations_v1");
            statement.executeUpdate("DROP TABLE changes_v1");
        }
    }

    private void migrateV2ToV3() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                "ALTER TABLE block_states ADD COLUMN reference_count INTEGER NOT NULL DEFAULT 0"
            );
        }
        rebuildStateReferenceCounts();
    }

    private void rebuildStateReferenceCounts() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP TABLE IF EXISTS temp.history_state_references");
            statement.executeUpdate("""
                CREATE TEMP TABLE history_state_references (
                    id INTEGER PRIMARY KEY,
                    amount INTEGER NOT NULL
                )
                """);
            accumulateExistingReferences(statement, "changes", "before_state_id");
            accumulateExistingReferences(statement, "changes", "after_state_id");
            accumulateExistingReferences(statement, "operation_items", "before_state_id");
            accumulateExistingReferences(statement, "operation_items", "after_state_id");
            statement.executeUpdate("""
                UPDATE block_states
                   SET reference_count = COALESCE(
                       (SELECT amount FROM history_state_references r WHERE r.id = block_states.id),
                       0
                   )
                """);
            statement.executeUpdate("DROP TABLE history_state_references");
        }
    }

    private static void accumulateExistingReferences(Statement statement, String table, String column)
        throws SQLException {
        statement.executeUpdate("""
            INSERT INTO history_state_references(id, amount)
            SELECT %1$s, COUNT(*) FROM %2$s GROUP BY %1$s
            ON CONFLICT(id) DO UPDATE SET amount = amount + excluded.amount
            """.formatted(column, table));
    }

    private void migrateV1Changes() throws SQLException {
        String selectSql = """
            SELECT id, occurred_at, world_uuid, x, y, z,
                   actor_uuid, actor_name, actor_kind, cause,
                   before_data, before_payload_type, before_payload,
                   after_data, after_payload_type, after_payload,
                   operation_uuid, metadata
              FROM changes_v1
             ORDER BY id
            """;
        try (Statement select = connection.createStatement();
             ResultSet result = select.executeQuery(selectSql);
             PreparedStatement insert = connection.prepareStatement(insertChangeSqlWithId())) {
            int pending = 0;
            while (result.next()) {
                ChangeRecord change = readLegacyChange(result);
                bindChange(insert, change, true);
                insert.addBatch();
                pending++;
                if (pending >= 1_000) {
                    insert.executeBatch();
                    pending = 0;
                }
            }
            if (pending > 0) {
                insert.executeBatch();
            }
        }
    }

    private void migrateV1Operations() throws SQLException {
        String selectSql = """
            SELECT operation_uuid, created_at, completed_at, actor_uuid, actor_name,
                   actor_kind, kind, status, summary, inverse_of, item_count,
                   applied_count, skipped_count, failure
              FROM operations_v1
            """;
        String insertSql = """
            INSERT INTO operations(
                operation_uuid, created_at, completed_at, actor_id, kind, status,
                summary, inverse_of, item_count, applied_count, skipped_count, failure
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
        try (Statement select = connection.createStatement();
             ResultSet result = select.executeQuery(selectSql);
             PreparedStatement insert = connection.prepareStatement(insertSql)) {
            while (result.next()) {
                ActorRef actor = new ActorRef(
                    UUID.fromString(result.getString("actor_uuid")),
                    result.getString("actor_name"),
                    ActorKind.valueOf(result.getString("actor_kind"))
                );
                insert.setBytes(1, UuidCodec.encode(UUID.fromString(result.getString("operation_uuid"))));
                insert.setLong(2, result.getLong("created_at"));
                setNullableLong(insert, 3, result, "completed_at");
                insert.setLong(4, actorId(actor));
                insert.setInt(5, OperationKind.valueOf(result.getString("kind")).storageCode());
                insert.setInt(6, OperationStatus.valueOf(result.getString("status")).storageCode());
                insert.setString(7, result.getString("summary"));
                setNullableUuidString(insert, 8, result.getString("inverse_of"));
                insert.setInt(9, result.getInt("item_count"));
                insert.setInt(10, result.getInt("applied_count"));
                insert.setInt(11, result.getInt("skipped_count"));
                setNullableText(insert, 12, result.getString("failure"));
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private void migrateV1OperationItems() throws SQLException {
        String selectSql = """
            SELECT operation_uuid, sequence, world_uuid, x, y, z,
                   before_data, before_payload_type, before_payload,
                   after_data, after_payload_type, after_payload, source_ids, applied
              FROM operation_items_v1
             ORDER BY operation_uuid, sequence
            """;
        try (Statement select = connection.createStatement();
             ResultSet result = select.executeQuery(selectSql);
             PreparedStatement insert = connection.prepareStatement(insertOperationItemSql())) {
            while (result.next()) {
                OperationItem item = new OperationItem(
                    result.getInt("sequence"),
                    new BlockPosition(
                        UUID.fromString(result.getString("world_uuid")),
                        result.getInt("x"),
                        result.getInt("y"),
                        result.getInt("z")
                    ),
                    readLegacySnapshot(result, "before"),
                    readLegacySnapshot(result, "after"),
                    parseLegacySourceIds(result.getString("source_ids"))
                );
                bindOperationItem(
                    insert,
                    UUID.fromString(result.getString("operation_uuid")),
                    item,
                    result.getInt("applied") != 0
                );
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    @Override
    public void insertBatch(List<ChangeRecord> changes) {
        if (changes.isEmpty()) {
            return;
        }
        try {
            inTransaction(() -> {
                Map<Long, Integer> references = new LinkedHashMap<>();
                try (PreparedStatement statement = connection.prepareStatement(insertChangeSql())) {
                    for (ChangeRecord change : changes) {
                        addStateReferences(references, bindChange(statement, change, false));
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                adjustStateReferences(references, 1);
                updateStorageMetrics(metricDeltas(changes));
            });
            changesSinceOptimize += changes.size();
            if (changesSinceOptimize >= OPTIMIZE_INTERVAL) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("PRAGMA optimize");
                }
                changesSinceOptimize = 0L;
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to persist a History change batch", exception);
        }
    }

    @Override
    public List<ChangeRecord> query(HistoryQuery query) {
        Long worldId = findWorldId(query.worldId());
        if (worldId == null) {
            return List.of();
        }
        StringBuilder sql = new StringBuilder("""
            SELECT c.id, c.occurred_at, c.chunk_x, c.chunk_z, c.packed_position,
                   a.uuid AS actor_uuid, a.name AS actor_name, a.kind AS actor_kind,
                   c.cause,
                   bs1.block_data AS before_data,
                   bs1.payload_type AS before_payload_type,
                   bs1.payload AS before_payload,
                   bs2.block_data AS after_data,
                   bs2.payload_type AS after_payload_type,
                   bs2.payload AS after_payload,
                   c.operation_uuid, eb.uuid AS batch_uuid, c.metadata
              FROM changes c
              JOIN actors a ON a.id = c.actor_id
              JOIN block_states bs1 ON bs1.id = c.before_state_id
              JOIN block_states bs2 ON bs2.id = c.after_state_id
              LEFT JOIN edit_batches eb ON eb.id = c.batch_id
             WHERE c.world_id = ? AND c.occurred_at >= ?
            """);
        if (query.exactPosition()) {
            sql.append(" AND c.chunk_x = ? AND c.chunk_z = ? AND c.packed_position = ?");
        } else {
            sql.append(" AND c.chunk_x BETWEEN ? AND ? AND c.chunk_z BETWEEN ? AND ?");
            sql.append(" AND (((c.chunk_x * 16 + (c.packed_position & 15)) - ?)");
            sql.append(" * ((c.chunk_x * 16 + (c.packed_position & 15)) - ?)");
            sql.append(" + ((c.chunk_z * 16 + ((c.packed_position >> 4) & 15)) - ?)");
            sql.append(" * ((c.chunk_z * 16 + ((c.packed_position >> 4) & 15)) - ?)) <= ?");
        }
        boolean filterActor = query.actor() != null;
        UUID actorUuid = filterActor ? parseUuid(query.actor()) : null;
        if (actorUuid != null) {
            sql.append(" AND a.uuid = ?");
        } else if (filterActor) {
            sql.append(" AND a.name = ? COLLATE NOCASE");
        }
        if (query.cause() != null) {
            sql.append(" AND c.cause = ?");
        }
        RollbackCauseFilterSql.append(sql, query);
        MaterialFilterSql.append(sql, query);
        if (query.hasCursor()) {
            sql.append(" AND (c.occurred_at < ? OR (c.occurred_at = ? AND c.id < ?))");
        }
        sql.append(" ORDER BY c.occurred_at DESC, c.id DESC LIMIT ?");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            statement.setLong(parameter++, worldId);
            statement.setLong(parameter++, query.since());
            if (query.exactPosition()) {
                BlockPosition position = new BlockPosition(
                    query.worldId(), query.exactX(), query.exactY(), query.exactZ()
                );
                statement.setInt(parameter++, position.chunkX());
                statement.setInt(parameter++, position.chunkZ());
                statement.setLong(parameter++, PositionCodec.pack(position));
            } else {
                statement.setInt(parameter++, (query.centerX() - query.radius()) >> 4);
                statement.setInt(parameter++, (query.centerX() + query.radius()) >> 4);
                statement.setInt(parameter++, (query.centerZ() - query.radius()) >> 4);
                statement.setInt(parameter++, (query.centerZ() + query.radius()) >> 4);
                statement.setInt(parameter++, query.centerX());
                statement.setInt(parameter++, query.centerX());
                statement.setInt(parameter++, query.centerZ());
                statement.setInt(parameter++, query.centerZ());
                statement.setLong(parameter++, (long) query.radius() * query.radius());
            }
            if (actorUuid != null) {
                statement.setBytes(parameter++, UuidCodec.encode(actorUuid));
            } else if (filterActor) {
                statement.setString(parameter++, query.actor());
            }
            if (query.cause() != null) {
                statement.setInt(parameter++, query.cause().storageCode());
            }
            parameter = RollbackCauseFilterSql.bind(statement, parameter, query);
            parameter = MaterialFilterSql.bind(statement, parameter, query);
            if (query.hasCursor()) {
                statement.setLong(parameter++, query.beforeOccurredAt());
                statement.setLong(parameter++, query.beforeOccurredAt());
                statement.setLong(parameter++, query.beforeId());
            }
            statement.setInt(parameter, query.limit());

            try (ResultSet result = statement.executeQuery()) {
                List<ChangeRecord> changes = new ArrayList<>();
                while (result.next()) {
                    changes.add(readChange(result, query.worldId()));
                }
                return List.copyOf(changes);
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to query History changes", exception);
        }
    }

    @Override
    public void scanRollbackChanges(HistoryQuery query, ChangeRecordSink sink) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(sink, "sink");
        if (!query.rollbackOnly() || query.hasCursor()) {
            throw new IllegalArgumentException("Streaming scans require an unpaged rollback query");
        }
        Long worldId = findWorldId(query.worldId());
        if (worldId == null) {
            return;
        }
        StringBuilder sql = new StringBuilder("""
            SELECT c.id, c.occurred_at, c.chunk_x, c.chunk_z, c.packed_position,
                   a.uuid AS actor_uuid, a.name AS actor_name, a.kind AS actor_kind,
                   c.cause,
                   bs1.block_data AS before_data,
                   bs1.payload_type AS before_payload_type,
                   bs1.payload AS before_payload,
                   bs2.block_data AS after_data,
                   bs2.payload_type AS after_payload_type,
                   bs2.payload AS after_payload,
                   c.operation_uuid, eb.uuid AS batch_uuid, c.metadata
              FROM changes c
              JOIN actors a ON a.id = c.actor_id
              JOIN block_states bs1 ON bs1.id = c.before_state_id
              JOIN block_states bs2 ON bs2.id = c.after_state_id
              LEFT JOIN edit_batches eb ON eb.id = c.batch_id
             WHERE c.world_id = ? AND c.occurred_at >= ?
            """);
        appendSpatialFilter(sql, query, false);
        boolean filterActor = query.actor() != null;
        UUID actorUuid = filterActor ? parseUuid(query.actor()) : null;
        if (actorUuid != null) {
            sql.append(" AND a.uuid = ?");
        } else if (filterActor) {
            sql.append(" AND a.name = ? COLLATE NOCASE");
        }
        if (query.cause() != null) {
            sql.append(" AND c.cause = ?");
        }
        RollbackCauseFilterSql.append(sql, query);
        MaterialFilterSql.append(sql, query);
        sql.append(" ORDER BY c.chunk_x, c.chunk_z, c.packed_position, c.occurred_at DESC, c.id DESC");

        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            statement.setFetchSize(query.limit());
            int parameter = bindQueryPrefix(statement, worldId, query, actorUuid, filterActor, false);
            RollbackCauseFilterSql.bind(statement, parameter, query);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    sink.accept(readChange(result, query.worldId()));
                }
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to stream SQLite rollback changes", exception);
        }
    }

    private static void appendSpatialFilter(StringBuilder sql, HistoryQuery query, boolean postgres) {
        if (query.exactPosition()) {
            sql.append(" AND c.chunk_x = ? AND c.chunk_z = ? AND c.packed_position = ?");
            return;
        }
        sql.append(" AND c.chunk_x BETWEEN ? AND ? AND c.chunk_z BETWEEN ? AND ?");
        String chunkX = postgres ? "(c.chunk_x::bigint * 16)" : "(c.chunk_x * 16)";
        String chunkZ = postgres ? "(c.chunk_z::bigint * 16)" : "(c.chunk_z * 16)";
        sql.append(" AND ((").append(chunkX).append(" + (c.packed_position & 15) - ?)");
        sql.append(" * (").append(chunkX).append(" + (c.packed_position & 15) - ?)");
        sql.append(" + (").append(chunkZ).append(" + ((c.packed_position >> 4) & 15) - ?)");
        sql.append(" * (").append(chunkZ).append(" + ((c.packed_position >> 4) & 15) - ?)) <= ?");
    }

    private int bindQueryPrefix(
        PreparedStatement statement,
        long worldId,
        HistoryQuery query,
        UUID actorUuid,
        boolean filterActor,
        boolean postgres
    ) throws SQLException {
        int parameter = 1;
        statement.setLong(parameter++, worldId);
        statement.setLong(parameter++, query.since());
        if (query.exactPosition()) {
            BlockPosition position = new BlockPosition(
                query.worldId(), query.exactX(), query.exactY(), query.exactZ()
            );
            statement.setInt(parameter++, position.chunkX());
            statement.setInt(parameter++, position.chunkZ());
            statement.setLong(parameter++, PositionCodec.pack(position));
        } else {
            statement.setInt(parameter++, Math.subtractExact(query.centerX(), query.radius()) >> 4);
            statement.setInt(parameter++, Math.addExact(query.centerX(), query.radius()) >> 4);
            statement.setInt(parameter++, Math.subtractExact(query.centerZ(), query.radius()) >> 4);
            statement.setInt(parameter++, Math.addExact(query.centerZ(), query.radius()) >> 4);
            statement.setLong(parameter++, query.centerX());
            statement.setLong(parameter++, query.centerX());
            statement.setLong(parameter++, query.centerZ());
            statement.setLong(parameter++, query.centerZ());
            statement.setLong(parameter++, Math.multiplyExact((long) query.radius(), query.radius()));
        }
        if (actorUuid != null) {
            if (postgres) {
                statement.setObject(parameter++, actorUuid);
            } else {
                statement.setBytes(parameter++, UuidCodec.encode(actorUuid));
            }
        } else if (filterActor) {
            statement.setString(parameter++, query.actor());
        }
        if (query.cause() != null) {
            statement.setInt(parameter++, query.cause().storageCode());
        }
        return parameter;
    }

    @Override
    public Map<BlockPosition, ChangeRecord> latestChanges(List<BlockPosition> positions) {
        if (positions.isEmpty()) {
            return Map.of();
        }
        UUID worldUuid = positions.getFirst().worldId();
        Set<BlockPosition> unique = new HashSet<>();
        for (BlockPosition position : positions) {
            if (!worldUuid.equals(position.worldId())) {
                throw new IllegalArgumentException("Latest-state lookup must remain inside one world");
            }
            if (!unique.add(position)) {
                throw new IllegalArgumentException("Latest-state lookup contains a duplicate position");
            }
        }
        Long worldId = findWorldId(worldUuid);
        if (worldId == null) {
            return Map.of();
        }
        Map<BlockPosition, ChangeRecord> latest = new LinkedHashMap<>();
        for (int start = 0; start < positions.size(); start += LATEST_POSITION_BATCH_SIZE) {
            int end = Math.min(start + LATEST_POSITION_BATCH_SIZE, positions.size());
            latest.putAll(latestChangesBatch(worldUuid, worldId, positions.subList(start, end)));
        }
        return Map.copyOf(latest);
    }

    private Map<BlockPosition, ChangeRecord> latestChangesBatch(
        UUID worldUuid,
        long worldId,
        List<BlockPosition> positions
    ) {
        StringBuilder sql = new StringBuilder(
            "WITH requested(chunk_x, chunk_z, packed_position) AS (VALUES "
        );
        for (int index = 0; index < positions.size(); index++) {
            if (index > 0) {
                sql.append(',');
            }
            sql.append("(?,?,?)");
        }
        sql.append("""
            ), ranked AS (
                SELECT c.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY c.chunk_x, c.chunk_z, c.packed_position
                           ORDER BY c.occurred_at DESC, c.id DESC
                       ) AS history_rank
                  FROM changes c
                  JOIN requested r
                    ON r.chunk_x = c.chunk_x
                   AND r.chunk_z = c.chunk_z
                   AND r.packed_position = c.packed_position
                 WHERE c.world_id = ?
            """);
        RollbackCauseFilterSql.appendEligible(sql, "c.cause");
        sql.append("""
            )
            SELECT c.id, c.occurred_at, c.chunk_x, c.chunk_z, c.packed_position,
                   a.uuid AS actor_uuid, a.name AS actor_name, a.kind AS actor_kind,
                   c.cause,
                   bs1.block_data AS before_data,
                   bs1.payload_type AS before_payload_type,
                   bs1.payload AS before_payload,
                   bs2.block_data AS after_data,
                   bs2.payload_type AS after_payload_type,
                   bs2.payload AS after_payload,
                   c.operation_uuid, eb.uuid AS batch_uuid, c.metadata
              FROM ranked c
              JOIN actors a ON a.id = c.actor_id
              JOIN block_states bs1 ON bs1.id = c.before_state_id
              JOIN block_states bs2 ON bs2.id = c.after_state_id
              LEFT JOIN edit_batches eb ON eb.id = c.batch_id
             WHERE c.history_rank = 1
            """);
        try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
            int parameter = 1;
            for (BlockPosition position : positions) {
                statement.setInt(parameter++, position.chunkX());
                statement.setInt(parameter++, position.chunkZ());
                statement.setLong(parameter++, PositionCodec.pack(position));
            }
            statement.setLong(parameter++, worldId);
            RollbackCauseFilterSql.bindEligible(statement, parameter);
            try (ResultSet result = statement.executeQuery()) {
                Map<BlockPosition, ChangeRecord> latest = new LinkedHashMap<>();
                while (result.next()) {
                    ChangeRecord change = readChange(result, worldUuid);
                    latest.put(change.position(), change);
                }
                return latest;
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to validate latest SQLite history states", exception);
        }
    }

    @Override
    public void prepareOperation(OperationDraft operation) {
        try {
            inTransaction(() -> {
                long actorId = actorId(operation.actor());
                try (PreparedStatement operationStatement = connection.prepareStatement("""
                        INSERT INTO operations(
                            operation_uuid, created_at, actor_id, kind, status,
                            summary, inverse_of, item_count
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """);
                     PreparedStatement itemStatement = connection.prepareStatement(insertOperationItemSql())) {
                    Map<Long, Integer> references = new LinkedHashMap<>();
                    operationStatement.setBytes(1, UuidCodec.encode(operation.id()));
                    operationStatement.setLong(2, operation.createdAt());
                    operationStatement.setLong(3, actorId);
                    operationStatement.setInt(4, operation.kind().storageCode());
                    operationStatement.setInt(5, OperationStatus.PREPARED.storageCode());
                    operationStatement.setString(6, operation.summary());
                    setNullableUuid(operationStatement, 7, operation.inverseOf());
                    operationStatement.setInt(8, operation.items().size());
                    operationStatement.executeUpdate();

                    for (OperationItem item : operation.items()) {
                        addStateReferences(
                            references,
                            bindOperationItem(itemStatement, operation.id(), item, false)
                        );
                        itemStatement.addBatch();
                    }
                    itemStatement.executeBatch();
                    adjustStateReferences(references, 1);
                }
            });
        } catch (SQLException exception) {
            throw new StorageException("Unable to prepare History operation", exception);
        }
    }

    @Override
    public void prepareOperation(OperationHeader operation, OperationItemSource items, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Operation preparation batch size must be positive");
        }
        try {
            inTransaction(() -> {
                long actorId = actorId(operation.actor());
                try (PreparedStatement operationStatement = connection.prepareStatement("""
                        INSERT INTO operations(
                            operation_uuid, created_at, actor_id, kind, status,
                            summary, inverse_of, item_count
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """);
                     PreparedStatement itemStatement = connection.prepareStatement(insertOperationItemSql())) {
                    operationStatement.setBytes(1, UuidCodec.encode(operation.id()));
                    operationStatement.setLong(2, operation.createdAt());
                    operationStatement.setLong(3, actorId);
                    operationStatement.setInt(4, operation.kind().storageCode());
                    operationStatement.setInt(5, OperationStatus.PREPARED.storageCode());
                    operationStatement.setString(6, operation.summary());
                    setNullableUuid(operationStatement, 7, operation.inverseOf());
                    operationStatement.setInt(8, operation.itemCount());
                    operationStatement.executeUpdate();

                    int inserted = 0;
                    while (true) {
                        List<OperationItem> batch = items.readBatch(batchSize);
                        if (batch.isEmpty()) {
                            break;
                        }
                        Map<Long, Integer> references = new LinkedHashMap<>();
                        for (OperationItem item : batch) {
                            addStateReferences(
                                references,
                                bindOperationItem(itemStatement, operation.id(), item, false)
                            );
                            itemStatement.addBatch();
                        }
                        itemStatement.executeBatch();
                        adjustStateReferences(references, 1);
                        inserted = Math.addExact(inserted, batch.size());
                    }
                    if (inserted != operation.itemCount()) {
                        throw new StorageException(
                            "Operation plan contained " + inserted + " items; expected " + operation.itemCount()
                        );
                    }
                }
            });
        } catch (SQLException exception) {
            throw new StorageException("Unable to prepare streamed History operation", exception);
        }
    }

    @Override
    public void checkpointOperation(OperationCheckpoint checkpoint) {
        try {
            inTransaction(() -> checkpointOperationInTransaction(checkpoint));
        } catch (SQLException exception) {
            throw new StorageException("Unable to checkpoint History operation", exception);
        }
    }

    private void checkpointOperationInTransaction(OperationCheckpoint checkpoint) throws SQLException {
        OperationSummary summary = loadOperationSummaryInternal(checkpoint.operationId())
            .orElseThrow(() -> new StorageException("Prepared operation was not found"));
        if (summary.status() != OperationStatus.PREPARED) {
            if (checkpoint.applied().stream().allMatch(applied -> operationItemApplied(
                checkpoint.operationId(),
                applied.item().sequence()
            ))) {
                return;
            }
            throw new StorageException("Operation is no longer in PREPARED state");
        }

        List<AppliedOperationItem> newlyApplied = new ArrayList<>();
        try (PreparedStatement itemStatement = connection.prepareStatement("""
                UPDATE operation_items SET applied = 1
                 WHERE operation_uuid = ? AND sequence = ? AND applied = 0
                """)) {
            for (AppliedOperationItem applied : checkpoint.applied()) {
                itemStatement.setBytes(1, UuidCodec.encode(checkpoint.operationId()));
                itemStatement.setInt(2, applied.item().sequence());
                itemStatement.addBatch();
            }
            int[] results = itemStatement.executeBatch();
            for (int index = 0; index < results.length; index++) {
                if (results[index] > 0 || results[index] == Statement.SUCCESS_NO_INFO) {
                    newlyApplied.add(checkpoint.applied().get(index));
                } else if (results[index] == Statement.EXECUTE_FAILED) {
                    throw new StorageException("Operation item checkpoint batch failed");
                }
            }
        }
        if (newlyApplied.isEmpty()) {
            return;
        }

        List<ChangeRecord> auditChanges = new ArrayList<>(newlyApplied.size());
        try (PreparedStatement changeStatement = connection.prepareStatement(insertChangeSql())) {
            Map<Long, Integer> references = new LinkedHashMap<>();
            ChangeCause cause = summary.header().kind() == OperationKind.ROLLBACK
                ? ChangeCause.HISTORY_ROLLBACK
                : ChangeCause.HISTORY_UNDO;
            for (AppliedOperationItem applied : newlyApplied) {
                ChangeRecord auditChange = new ChangeRecord(
                    0L,
                    checkpoint.checkpointAt(),
                    applied.item().position(),
                    summary.header().actor(),
                    cause,
                    applied.actualBefore(),
                    applied.actualAfter(),
                    checkpoint.operationId(),
                    null,
                    "history-operation"
                );
                auditChanges.add(auditChange);
                addStateReferences(references, bindChange(changeStatement, auditChange, false));
                changeStatement.addBatch();
            }
            changeStatement.executeBatch();
            adjustStateReferences(references, 1);
            updateStorageMetrics(metricDeltas(auditChanges));
        }
        try (PreparedStatement statement = connection.prepareStatement("""
            UPDATE operations SET applied_count = applied_count + ?
             WHERE operation_uuid = ? AND status = ?
            """)) {
            statement.setInt(1, newlyApplied.size());
            statement.setBytes(2, UuidCodec.encode(checkpoint.operationId()));
            statement.setInt(3, OperationStatus.PREPARED.storageCode());
            if (statement.executeUpdate() != 1) {
                throw new StorageException("Prepared operation state changed during checkpoint");
            }
        }
    }

    @Override
    public void finalizeOperation(OperationFinalization finalization) {
        try {
            inTransaction(() -> {
                try (PreparedStatement statement = connection.prepareStatement("""
                    UPDATE operations
                       SET completed_at = ?, status = ?, skipped_count = ?, failure = ?
                     WHERE operation_uuid = ? AND status = ?
                    """)) {
                    statement.setLong(1, finalization.completedAt());
                    statement.setInt(2, finalization.status().storageCode());
                    statement.setInt(3, finalization.skipped());
                    setNullableText(statement, 4, finalization.failure());
                    statement.setBytes(5, UuidCodec.encode(finalization.operationId()));
                    statement.setInt(6, OperationStatus.PREPARED.storageCode());
                    if (statement.executeUpdate() == 1) {
                        return;
                    }
                }
                OperationSummary existing = loadOperationSummaryInternal(finalization.operationId())
                    .orElseThrow(() -> new StorageException("Operation was not found during finalization"));
                if (existing.status() != finalization.status()
                    || existing.skippedCount() != finalization.skipped()
                    || !existing.failure().equals(finalization.failure())) {
                    throw new StorageException("Operation finalization conflicts with its durable state");
                }
            });
        } catch (SQLException exception) {
            throw new StorageException("Unable to finalize History operation", exception);
        }
    }

    @Override
    public void completeOperation(OperationCompletion completion) {
        try {
            inTransaction(() -> {
                StoredOperation stored = loadOperationInternal(completion.operationId())
                    .orElseThrow(() -> new StorageException("Prepared operation was not found"));
                if (stored.status() != OperationStatus.PREPARED) {
                    throw new StorageException("Operation is no longer in PREPARED state");
                }

                try (PreparedStatement itemStatement = connection.prepareStatement("""
                        UPDATE operation_items SET applied = 1
                         WHERE operation_uuid = ? AND sequence = ?
                        """);
                     PreparedStatement changeStatement = connection.prepareStatement(insertChangeSql())) {
                    Map<Long, Integer> references = new LinkedHashMap<>();
                    List<ChangeRecord> auditChanges = new ArrayList<>();
                    ChangeCause cause = stored.draft().kind() == OperationKind.ROLLBACK
                        ? ChangeCause.HISTORY_ROLLBACK
                        : ChangeCause.HISTORY_UNDO;
                    for (AppliedOperationItem applied : completion.applied()) {
                        itemStatement.setBytes(1, UuidCodec.encode(completion.operationId()));
                        itemStatement.setInt(2, applied.item().sequence());
                        itemStatement.addBatch();

                        ChangeRecord auditChange = new ChangeRecord(
                            0L,
                            completion.completedAt(),
                            applied.item().position(),
                            stored.draft().actor(),
                            cause,
                            applied.actualBefore(),
                            applied.actualAfter(),
                            completion.operationId(),
                            null,
                            "history-operation"
                        );
                        auditChanges.add(auditChange);
                        addStateReferences(references, bindChange(changeStatement, auditChange, false));
                        changeStatement.addBatch();
                    }
                    itemStatement.executeBatch();
                    changeStatement.executeBatch();
                    adjustStateReferences(references, 1);
                    updateStorageMetrics(metricDeltas(auditChanges));
                }

                try (PreparedStatement statement = connection.prepareStatement("""
                    UPDATE operations
                       SET completed_at = ?, status = ?, applied_count = ?, skipped_count = ?, failure = ?
                     WHERE operation_uuid = ? AND status = ?
                    """)) {
                    statement.setLong(1, completion.completedAt());
                    statement.setInt(2, completion.status().storageCode());
                    statement.setInt(3, completion.applied().size());
                    statement.setInt(4, completion.skipped());
                    setNullableText(statement, 5, completion.failure());
                    statement.setBytes(6, UuidCodec.encode(completion.operationId()));
                    statement.setInt(7, OperationStatus.PREPARED.storageCode());
                    if (statement.executeUpdate() != 1) {
                        throw new StorageException("Prepared operation state changed during completion");
                    }
                }
            });
        } catch (SQLException exception) {
            throw new StorageException("Unable to complete History operation", exception);
        }
    }

    @Override
    public Optional<StoredOperation> loadOperation(UUID operationId) {
        try {
            return loadOperationInternal(operationId);
        } catch (SQLException exception) {
            throw new StorageException("Unable to load History operation", exception);
        }
    }

    @Override
    public Optional<StoredOperation> findLastOperation(UUID actorId) {
        String sql = """
            SELECT o.operation_uuid
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE a.uuid = ? AND o.status IN (?, ?)
             ORDER BY o.created_at DESC
             LIMIT 1
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBytes(1, UuidCodec.encode(actorId));
            statement.setInt(2, OperationStatus.APPLIED.storageCode());
            statement.setInt(3, OperationStatus.PARTIAL.storageCode());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                return loadOperationInternal(UuidCodec.decode(result.getBytes(1)));
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to find the last History operation", exception);
        }
    }

    @Override
    public Optional<OperationSummary> loadOperationSummary(UUID operationId) {
        try {
            return loadOperationSummaryInternal(operationId);
        } catch (SQLException exception) {
            throw new StorageException("Unable to load History operation summary", exception);
        }
    }

    @Override
    public Optional<OperationSummary> findLastOperationSummary(UUID actorId) {
        String sql = """
            SELECT o.operation_uuid
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE a.uuid = ? AND o.status IN (?, ?)
             ORDER BY o.created_at DESC
             LIMIT 1
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBytes(1, UuidCodec.encode(actorId));
            statement.setInt(2, OperationStatus.APPLIED.storageCode());
            statement.setInt(3, OperationStatus.PARTIAL.storageCode());
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                    ? loadOperationSummaryInternal(UuidCodec.decode(result.getBytes(1)))
                    : Optional.empty();
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to find the last History operation summary", exception);
        }
    }

    @Override
    public void scanAppliedOperationItems(UUID operationId, OperationItemSink sink) {
        scanOperationItems(operationId, true, sink);
    }

    @Override
    public void scanPendingOperationItems(UUID operationId, OperationItemSink sink) {
        scanOperationItems(operationId, false, sink);
    }

    private void scanOperationItems(UUID operationId, boolean applied, OperationItemSink sink) {
        Objects.requireNonNull(operationId, "operationId");
        Objects.requireNonNull(sink, "sink");
        String sql = """
            SELECT i.sequence, w.uuid AS world_uuid, i.chunk_x, i.chunk_z, i.packed_position,
                   bs1.block_data AS before_data,
                   bs1.payload_type AS before_payload_type,
                   bs1.payload AS before_payload,
                   bs2.block_data AS after_data,
                   bs2.payload_type AS after_payload_type,
                   bs2.payload AS after_payload,
                   i.source_ids
              FROM operation_items i
              JOIN worlds w ON w.id = i.world_id
              JOIN block_states bs1 ON bs1.id = i.before_state_id
              JOIN block_states bs2 ON bs2.id = i.after_state_id
             WHERE i.operation_uuid = ? AND i.applied = ?
             ORDER BY i.chunk_x, i.chunk_z, i.packed_position, i.sequence
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBytes(1, UuidCodec.encode(operationId));
            statement.setInt(2, applied ? 1 : 0);
            statement.setFetchSize(2_048);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    sink.accept(readOperationItem(result));
                }
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to stream History operation items", exception);
        }
    }

    private Optional<OperationSummary> loadOperationSummaryInternal(UUID operationId) throws SQLException {
        String sql = """
            SELECT o.created_at, a.uuid AS actor_uuid, a.name AS actor_name,
                   a.kind AS actor_kind, o.kind, o.status, o.summary, o.inverse_of,
                   o.item_count, o.applied_count, o.skipped_count, o.failure
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE o.operation_uuid = ?
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBytes(1, UuidCodec.encode(operationId));
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                ActorRef actor = new ActorRef(
                    UuidCodec.decode(result.getBytes("actor_uuid")),
                    result.getString("actor_name"),
                    ActorKind.fromStorageCode(result.getInt("actor_kind"))
                );
                OperationHeader header = new OperationHeader(
                    operationId,
                    result.getLong("created_at"),
                    actor,
                    OperationKind.fromStorageCode(result.getInt("kind")),
                    result.getString("summary"),
                    UuidCodec.decode(result.getBytes("inverse_of")),
                    result.getInt("item_count")
                );
                return Optional.of(new OperationSummary(
                    header,
                    OperationStatus.fromStorageCode(result.getInt("status")),
                    result.getInt("applied_count"),
                    result.getInt("skipped_count"),
                    emptyIfNull(result.getString("failure"))
                ));
            }
        }
    }

    private OperationItem readOperationItem(ResultSet result) throws SQLException {
        UUID worldId = UuidCodec.decode(result.getBytes("world_uuid"));
        return new OperationItem(
            result.getInt("sequence"),
            PositionCodec.unpack(
                worldId,
                result.getInt("chunk_x"),
                result.getInt("chunk_z"),
                result.getLong("packed_position")
            ),
            readSnapshot(result, "before"),
            readSnapshot(result, "after"),
            SourceIdCodec.decode(result.getBytes("source_ids"))
        );
    }

    private boolean operationItemApplied(UUID operationId, int sequence) {
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT applied FROM operation_items WHERE operation_uuid = ? AND sequence = ?
            """)) {
            statement.setBytes(1, UuidCodec.encode(operationId));
            statement.setInt(2, sequence);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getInt(1) != 0;
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to verify a History operation checkpoint", exception);
        }
    }

    @Override
    public int interruptedOperationCount() {
        try (PreparedStatement statement = connection.prepareStatement(
            "SELECT COUNT(*) FROM operations WHERE status = ?"
        )) {
            statement.setInt(1, OperationStatus.PREPARED.storageCode());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getInt(1) : 0;
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to count interrupted History operations", exception);
        }
    }

    @Override
    public List<UUID> interruptedOperationIds(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT operation_uuid FROM operations
             WHERE status = ?
             ORDER BY created_at DESC, operation_uuid
             LIMIT ?
            """)) {
            statement.setInt(1, OperationStatus.PREPARED.storageCode());
            statement.setInt(2, limit);
            List<UUID> ids = new ArrayList<>();
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    ids.add(UuidCodec.decode(result.getBytes(1)));
                }
            }
            return List.copyOf(ids);
        } catch (SQLException exception) {
            throw new StorageException("Unable to list interrupted History operations", exception);
        }
    }

    @Override
    public int purgeChangesBefore(long cutoffMillis, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        try {
            final int[] deleted = new int[1];
            inTransaction(() -> {
                Map<Long, Integer> references = new LinkedHashMap<>();
                try (PreparedStatement statement = connection.prepareStatement("""
                    DELETE FROM changes
                     WHERE id IN (
                         SELECT id FROM changes
                          WHERE occurred_at < ?
                          ORDER BY id
                          LIMIT ?
                     )
                    RETURNING before_state_id, after_state_id
                    """)) {
                    statement.setLong(1, cutoffMillis);
                    statement.setInt(2, limit);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            addStateReferences(
                                references,
                                new StateIdPair(result.getLong(1), result.getLong(2))
                            );
                            deleted[0]++;
                        }
                    }
                }
                adjustStateReferences(references, -1);
                deleteUnreferencedStateCandidates(references);
            });
            if (deleted[0] > 0) {
                stateIds.clear();
                try (Statement statement = connection.createStatement()) {
                    statement.execute("PRAGMA incremental_vacuum(256)");
                    statement.execute("PRAGMA optimize");
                }
            }
            return deleted[0];
        } catch (SQLException exception) {
            throw new StorageException("Unable to purge expired History changes", exception);
        }
    }

    private void deleteUnreferencedStateCandidates(Map<Long, Integer> references) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
            "DELETE FROM block_states WHERE id = ? AND reference_count = 0"
        )) {
            for (Long stateId : references.keySet()) {
                statement.setLong(1, stateId);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    @Override
    public StorageProfile storageProfile() {
        try {
            List<CauseStorageMetric> metrics = new ArrayList<>();
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("""
                     SELECT cause, change_count, estimated_input_bytes,
                            first_occurred_at, last_occurred_at
                       FROM storage_metrics
                      ORDER BY estimated_input_bytes DESC, cause
                     """)) {
                while (result.next()) {
                    metrics.add(new CauseStorageMetric(
                        ChangeCause.fromStorageCode(result.getInt("cause")),
                        result.getLong("change_count"),
                        result.getLong("estimated_input_bytes"),
                        result.getLong("first_occurred_at"),
                        result.getLong("last_occurred_at")
                    ));
                }
            }
            long databaseBytes = fileSize(databaseFile);
            Path walFile = databaseFile.resolveSibling(databaseFile.getFileName() + "-wal");
            return new StorageProfile("SQLite", databaseBytes, fileSize(walFile), metrics);
        } catch (SQLException | java.io.IOException exception) {
            throw new StorageException("Unable to profile History SQLite storage", exception);
        }
    }

    @Override
    public String backendName() {
        return "SQLite";
    }

    @Override
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            try (Statement statement = connection.createStatement()) {
                statement.execute("PRAGMA optimize");
                statement.execute("PRAGMA wal_checkpoint(TRUNCATE)");
            }
            connection.close();
        } catch (SQLException exception) {
            throw new StorageException("Unable to close the History database", exception);
        } finally {
            connection = null;
            clearCaches();
        }
    }

    private Optional<StoredOperation> loadOperationInternal(UUID operationId) throws SQLException {
        String operationSql = """
            SELECT o.created_at, a.uuid AS actor_uuid, a.name AS actor_name,
                   a.kind AS actor_kind, o.kind, o.status, o.summary, o.inverse_of,
                   o.applied_count, o.skipped_count, o.failure
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE o.operation_uuid = ?
            """;
        try (PreparedStatement statement = connection.prepareStatement(operationSql)) {
            statement.setBytes(1, UuidCodec.encode(operationId));
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                ActorRef actor = new ActorRef(
                    UuidCodec.decode(result.getBytes("actor_uuid")),
                    result.getString("actor_name"),
                    ActorKind.fromStorageCode(result.getInt("actor_kind"))
                );
                OperationKind kind = OperationKind.fromStorageCode(result.getInt("kind"));
                OperationStatus status = OperationStatus.fromStorageCode(result.getInt("status"));
                UUID inverseOf = UuidCodec.decode(result.getBytes("inverse_of"));
                List<OperationItem> items = loadOperationItems(operationId, status);
                OperationDraft draft = new OperationDraft(
                    operationId,
                    result.getLong("created_at"),
                    actor,
                    kind,
                    result.getString("summary"),
                    inverseOf,
                    items
                );
                return Optional.of(new StoredOperation(
                    draft,
                    status,
                    result.getInt("applied_count"),
                    result.getInt("skipped_count"),
                    emptyIfNull(result.getString("failure"))
                ));
            }
        }
    }

    private List<OperationItem> loadOperationItems(UUID operationId, OperationStatus status) throws SQLException {
        String sql = """
            SELECT i.sequence, w.uuid AS world_uuid, i.chunk_x, i.chunk_z, i.packed_position,
                   bs1.block_data AS before_data,
                   bs1.payload_type AS before_payload_type,
                   bs1.payload AS before_payload,
                   bs2.block_data AS after_data,
                   bs2.payload_type AS after_payload_type,
                   bs2.payload AS after_payload,
                   i.source_ids, i.applied
              FROM operation_items i
              JOIN worlds w ON w.id = i.world_id
              JOIN block_states bs1 ON bs1.id = i.before_state_id
              JOIN block_states bs2 ON bs2.id = i.after_state_id
             WHERE i.operation_uuid = ?
             ORDER BY i.sequence
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setBytes(1, UuidCodec.encode(operationId));
            try (ResultSet result = statement.executeQuery()) {
                List<OperationItem> items = new ArrayList<>();
                while (result.next()) {
                    boolean appliedOnly = status == OperationStatus.APPLIED || status == OperationStatus.PARTIAL;
                    if (appliedOnly && result.getInt("applied") == 0) {
                        continue;
                    }
                    UUID worldId = UuidCodec.decode(result.getBytes("world_uuid"));
                    items.add(new OperationItem(
                        result.getInt("sequence"),
                        PositionCodec.unpack(
                            worldId,
                            result.getInt("chunk_x"),
                            result.getInt("chunk_z"),
                            result.getLong("packed_position")
                        ),
                        readSnapshot(result, "before"),
                        readSnapshot(result, "after"),
                        SourceIdCodec.decode(result.getBytes("source_ids"))
                    ));
                }
                return List.copyOf(items);
            }
        }
    }

    private String insertChangeSql() {
        return """
            INSERT INTO changes(
                occurred_at, world_id, chunk_x, chunk_z, packed_position,
                actor_id, cause, before_state_id, after_state_id,
                operation_uuid, batch_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    }

    private String insertChangeSqlWithId() {
        return """
            INSERT INTO changes(
                id, occurred_at, world_id, chunk_x, chunk_z, packed_position,
                actor_id, cause, before_state_id, after_state_id,
                operation_uuid, batch_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    }

    private String insertOperationItemSql() {
        return """
            INSERT INTO operation_items(
                operation_uuid, sequence, world_id, chunk_x, chunk_z, packed_position,
                before_state_id, after_state_id, source_ids, applied
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    }

    private StateIdPair bindChange(PreparedStatement statement, ChangeRecord change, boolean includeId)
        throws SQLException {
        int parameter = 1;
        if (includeId) {
            statement.setLong(parameter++, change.id());
        }
        statement.setLong(parameter++, change.occurredAt());
        statement.setLong(parameter++, worldId(change.position().worldId()));
        statement.setInt(parameter++, change.position().chunkX());
        statement.setInt(parameter++, change.position().chunkZ());
        statement.setLong(parameter++, PositionCodec.pack(change.position()));
        statement.setLong(parameter++, actorId(change.actor()));
        statement.setInt(parameter++, change.cause().storageCode());
        long beforeStateId = stateId(change.before());
        long afterStateId = stateId(change.after());
        statement.setLong(parameter++, beforeStateId);
        statement.setLong(parameter++, afterStateId);
        setNullableUuid(statement, parameter++, change.operationId());
        setNullableBatchId(statement, parameter++, change.batchId());
        setNullableText(statement, parameter, change.metadata());
        return new StateIdPair(beforeStateId, afterStateId);
    }

    private StateIdPair bindOperationItem(
        PreparedStatement statement,
        UUID operationId,
        OperationItem item,
        boolean applied
    ) throws SQLException {
        int parameter = 1;
        statement.setBytes(parameter++, UuidCodec.encode(operationId));
        statement.setInt(parameter++, item.sequence());
        statement.setLong(parameter++, worldId(item.position().worldId()));
        statement.setInt(parameter++, item.position().chunkX());
        statement.setInt(parameter++, item.position().chunkZ());
        statement.setLong(parameter++, PositionCodec.pack(item.position()));
        long beforeStateId = stateId(item.before());
        long afterStateId = stateId(item.after());
        statement.setLong(parameter++, beforeStateId);
        statement.setLong(parameter++, afterStateId);
        byte[] sourceIds = SourceIdCodec.encode(item.sourceIds());
        if (sourceIds.length == 0) {
            statement.setNull(parameter++, Types.BLOB);
        } else {
            statement.setBytes(parameter++, sourceIds);
        }
        statement.setInt(parameter, applied ? 1 : 0);
        return new StateIdPair(beforeStateId, afterStateId);
    }

    private void updateStorageMetrics(Map<ChangeCause, MetricDelta> deltas) throws SQLException {
        if (deltas.isEmpty()) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement("""
            INSERT INTO storage_metrics(
                cause, change_count, estimated_input_bytes, first_occurred_at, last_occurred_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(cause) DO UPDATE SET
                change_count = change_count + excluded.change_count,
                estimated_input_bytes = estimated_input_bytes + excluded.estimated_input_bytes,
                first_occurred_at = MIN(first_occurred_at, excluded.first_occurred_at),
                last_occurred_at = MAX(last_occurred_at, excluded.last_occurred_at)
            """)) {
            for (Map.Entry<ChangeCause, MetricDelta> entry : deltas.entrySet()) {
                MetricDelta delta = entry.getValue();
                statement.setInt(1, entry.getKey().storageCode());
                statement.setLong(2, delta.count());
                statement.setLong(3, delta.bytes());
                statement.setLong(4, delta.firstOccurredAt());
                statement.setLong(5, delta.lastOccurredAt());
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static Map<ChangeCause, MetricDelta> metricDeltas(List<ChangeRecord> changes) {
        Map<ChangeCause, MetricDelta> deltas = new LinkedHashMap<>();
        for (ChangeRecord change : changes) {
            deltas.merge(
                change.cause(),
                new MetricDelta(
                    1L,
                    StorageFootprintEstimator.estimate(change),
                    change.occurredAt(),
                    change.occurredAt()
                ),
                MetricDelta::merge
            );
        }
        return deltas;
    }

    private static long fileSize(Path file) throws java.io.IOException {
        return Files.exists(file) ? Files.size(file) : 0L;
    }

    private static void addStateReferences(Map<Long, Integer> references, StateIdPair pair) {
        references.merge(pair.before(), 1, Math::addExact);
        references.merge(pair.after(), 1, Math::addExact);
    }

    private void adjustStateReferences(Map<Long, Integer> references, int direction) throws SQLException {
        if (references.isEmpty()) {
            return;
        }
        try (PreparedStatement statement = connection.prepareStatement("""
            UPDATE block_states SET reference_count = reference_count + ? WHERE id = ?
            """)) {
            for (Map.Entry<Long, Integer> entry : references.entrySet()) {
                statement.setInt(1, Math.multiplyExact(entry.getValue(), direction));
                statement.setLong(2, entry.getKey());
                statement.addBatch();
            }
            int[] results = statement.executeBatch();
            for (int result : results) {
                if (result == 0) {
                    throw new StorageException("A referenced block state was not found");
                }
            }
        }
    }

    private ChangeRecord readChange(ResultSet result, UUID worldId) throws SQLException {
        ActorRef actor = new ActorRef(
            UuidCodec.decode(result.getBytes("actor_uuid")),
            result.getString("actor_name"),
            ActorKind.fromStorageCode(result.getInt("actor_kind"))
        );
        BlockPosition position = PositionCodec.unpack(
            worldId,
            result.getInt("chunk_x"),
            result.getInt("chunk_z"),
            result.getLong("packed_position")
        );
        return new ChangeRecord(
            result.getLong("id"),
            result.getLong("occurred_at"),
            position,
            actor,
            ChangeCause.fromStorageCode(result.getInt("cause")),
            readSnapshot(result, "before"),
            readSnapshot(result, "after"),
            UuidCodec.decode(result.getBytes("operation_uuid")),
            UuidCodec.decode(result.getBytes("batch_uuid")),
            emptyIfNull(result.getString("metadata"))
        );
    }

    private static ChangeRecord readLegacyChange(ResultSet result) throws SQLException {
        UUID actorUuid = parseNullableUuid(result.getString("actor_uuid"));
        ActorRef actor = new ActorRef(
            actorUuid,
            result.getString("actor_name"),
            ActorKind.valueOf(result.getString("actor_kind"))
        );
        return new ChangeRecord(
            result.getLong("id"),
            result.getLong("occurred_at"),
            new BlockPosition(
                UUID.fromString(result.getString("world_uuid")),
                result.getInt("x"),
                result.getInt("y"),
                result.getInt("z")
            ),
            actor,
            ChangeCause.valueOf(result.getString("cause")),
            readLegacySnapshot(result, "before"),
            readLegacySnapshot(result, "after"),
            parseNullableUuid(result.getString("operation_uuid")),
            null,
            emptyIfNull(result.getString("metadata"))
        );
    }

    private static BlockSnapshot readSnapshot(ResultSet result, String prefix) throws SQLException {
        return StoredSnapshotCodec.decode(
            result.getString(prefix + "_data"),
            emptyIfNull(result.getString(prefix + "_payload_type")),
            bytesIfNull(result.getBytes(prefix + "_payload"))
        );
    }

    private static BlockSnapshot readLegacySnapshot(ResultSet result, String prefix) throws SQLException {
        return new BlockSnapshot(
            result.getString(prefix + "_data"),
            emptyIfNull(result.getString(prefix + "_payload_type")),
            bytesIfNull(result.getBytes(prefix + "_payload"))
        );
    }

    private long worldId(UUID worldUuid) throws SQLException {
        Long cached = worldIds.get(worldUuid);
        if (cached != null) {
            return cached;
        }
        Long existing = findWorldId(worldUuid);
        if (existing != null) {
            worldIds.put(worldUuid, existing);
            return existing;
        }
        try (PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO worlds(uuid) VALUES (?)",
            Statement.RETURN_GENERATED_KEYS
        )) {
            statement.setBytes(1, UuidCodec.encode(worldUuid));
            statement.executeUpdate();
            long id = generatedId(statement, "world");
            worldIds.put(worldUuid, id);
            return id;
        }
    }

    private Long findWorldId(UUID worldUuid) {
        Long cached = worldIds.get(worldUuid);
        if (cached != null) {
            return cached;
        }
        try (PreparedStatement statement = connection.prepareStatement(
            "SELECT id FROM worlds WHERE uuid = ?"
        )) {
            statement.setBytes(1, UuidCodec.encode(worldUuid));
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                long id = result.getLong(1);
                worldIds.put(worldUuid, id);
                return id;
            }
        } catch (SQLException exception) {
            throw new StorageException("Unable to resolve a History world", exception);
        }
    }

    private long actorId(ActorRef actor) throws SQLException {
        Long cached = actorIds.get(actor);
        if (cached != null) {
            return cached;
        }
        String selectSql = actor.uuid() == null
            ? "SELECT id, name FROM actors WHERE uuid IS NULL AND name = ? COLLATE NOCASE AND kind = ?"
            : "SELECT id, name FROM actors WHERE uuid = ? AND kind = ?";
        try (PreparedStatement select = connection.prepareStatement(selectSql)) {
            if (actor.uuid() == null) {
                select.setString(1, actor.name());
            } else {
                select.setBytes(1, UuidCodec.encode(actor.uuid()));
            }
            select.setInt(2, actor.kind().storageCode());
            try (ResultSet result = select.executeQuery()) {
                if (result.next()) {
                    long id = result.getLong("id");
                    String storedName = result.getString("name");
                    if (actor.uuid() != null && !storedName.equals(actor.name())) {
                        try (PreparedStatement update = connection.prepareStatement(
                            "UPDATE actors SET name = ? WHERE id = ?"
                        )) {
                            update.setString(1, actor.name());
                            update.setLong(2, id);
                            update.executeUpdate();
                        }
                    }
                    actorIds.put(actor, id);
                    return id;
                }
            }
        }
        try (PreparedStatement insert = connection.prepareStatement(
            "INSERT INTO actors(uuid, name, kind) VALUES (?, ?, ?)",
            Statement.RETURN_GENERATED_KEYS
        )) {
            if (actor.uuid() == null) {
                insert.setNull(1, Types.BLOB);
            } else {
                insert.setBytes(1, UuidCodec.encode(actor.uuid()));
            }
            insert.setString(2, actor.name());
            insert.setInt(3, actor.kind().storageCode());
            insert.executeUpdate();
            long id = generatedId(insert, "actor");
            actorIds.put(actor, id);
            return id;
        }
    }

    private long stateId(BlockSnapshot snapshot) throws SQLException {
        boolean cacheable = StateCachePolicy.shouldCache(snapshot);
        Long cached = cacheable ? stateIds.get(snapshot) : null;
        if (cached != null) {
            return cached;
        }
        byte[] fingerprint = fingerprint(snapshot);
        try (PreparedStatement select = connection.prepareStatement("""
            SELECT id, block_data, payload_type, payload
              FROM block_states WHERE fingerprint = ?
            """)) {
            select.setBytes(1, fingerprint);
            try (ResultSet result = select.executeQuery()) {
                if (result.next()) {
                    BlockSnapshot stored = StoredSnapshotCodec.decode(
                        result.getString("block_data"),
                        emptyIfNull(result.getString("payload_type")),
                        bytesIfNull(result.getBytes("payload"))
                    );
                    if (!stored.equals(snapshot)) {
                        throw new StorageException("A block-state fingerprint collision was detected");
                    }
                    long id = result.getLong("id");
                    if (cacheable) {
                        stateIds.put(snapshot, id);
                    }
                    return id;
                }
            }
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO block_states(fingerprint, block_data, payload_type, payload)
                VALUES (?, ?, ?, ?)
                """, Statement.RETURN_GENERATED_KEYS)) {
            insert.setBytes(1, fingerprint);
            insert.setString(2, snapshot.blockData());
            StoredSnapshotCodec.Encoded encoded = StoredSnapshotCodec.encode(snapshot);
            if (encoded.payloadType().isEmpty()) {
                insert.setNull(3, Types.VARCHAR);
                insert.setNull(4, Types.BLOB);
            } else {
                insert.setString(3, encoded.payloadType());
                insert.setBytes(4, encoded.payload());
            }
            insert.executeUpdate();
            long id = generatedId(insert, "block state");
            if (cacheable) {
                stateIds.put(snapshot, id);
            }
            return id;
        }
    }

    private long batchId(UUID batchUuid) throws SQLException {
        Long cached = batchIds.get(batchUuid);
        if (cached != null) {
            return cached;
        }
        try (PreparedStatement select = connection.prepareStatement(
            "SELECT id FROM edit_batches WHERE uuid = ?"
        )) {
            select.setBytes(1, UuidCodec.encode(batchUuid));
            try (ResultSet result = select.executeQuery()) {
                if (result.next()) {
                    long id = result.getLong(1);
                    batchIds.put(batchUuid, id);
                    return id;
                }
            }
        }
        try (PreparedStatement insert = connection.prepareStatement(
            "INSERT INTO edit_batches(uuid) VALUES (?)",
            Statement.RETURN_GENERATED_KEYS
        )) {
            insert.setBytes(1, UuidCodec.encode(batchUuid));
            insert.executeUpdate();
            long id = generatedId(insert, "edit batch");
            batchIds.put(batchUuid, id);
            return id;
        }
    }

    private static byte[] fingerprint(BlockSnapshot snapshot) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateLengthPrefixed(digest, snapshot.blockData().getBytes(StandardCharsets.UTF_8));
            updateLengthPrefixed(digest, snapshot.payloadType().getBytes(StandardCharsets.UTF_8));
            updateLengthPrefixed(digest, snapshot.payload());
            return digest.digest();
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
    }

    private static void updateLengthPrefixed(MessageDigest digest, byte[] value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value.length).array());
        digest.update(value);
    }

    private static long generatedId(PreparedStatement statement, String entity) throws SQLException {
        try (ResultSet keys = statement.getGeneratedKeys()) {
            if (!keys.next()) {
                throw new StorageException("SQLite did not return the generated " + entity + " ID");
            }
            return keys.getLong(1);
        }
    }

    private void inTransaction(SqlWork work) throws SQLException {
        boolean previousAutoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            work.run();
            connection.commit();
        } catch (SQLException | RuntimeException exception) {
            try {
                connection.rollback();
            } finally {
                clearCaches();
            }
            throw exception;
        } finally {
            connection.setAutoCommit(previousAutoCommit);
        }
    }

    private static void setNullableUuid(PreparedStatement statement, int parameter, UUID value)
        throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.BLOB);
        } else {
            statement.setBytes(parameter, UuidCodec.encode(value));
        }
    }

    private void setNullableBatchId(PreparedStatement statement, int parameter, UUID value)
        throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.BIGINT);
        } else {
            statement.setLong(parameter, batchId(value));
        }
    }

    private static void setNullableUuidString(PreparedStatement statement, int parameter, String value)
        throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.BLOB);
        } else {
            statement.setBytes(parameter, UuidCodec.encode(UUID.fromString(value)));
        }
    }

    private static void setNullableText(PreparedStatement statement, int parameter, String value)
        throws SQLException {
        if (value == null || value.isEmpty()) {
            statement.setNull(parameter, Types.VARCHAR);
        } else {
            statement.setString(parameter, value);
        }
    }

    private static void setNullableLong(
        PreparedStatement statement,
        int parameter,
        ResultSet result,
        String column
    ) throws SQLException {
        long value = result.getLong(column);
        if (result.wasNull()) {
            statement.setNull(parameter, Types.BIGINT);
        } else {
            statement.setLong(parameter, value);
        }
    }

    private static UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static UUID parseNullableUuid(String value) {
        return value == null ? null : UUID.fromString(value);
    }

    private static List<Long> parseLegacySourceIds(String value) {
        if (value == null || value.isBlank()) {
            return List.of();
        }
        return Arrays.stream(value.split(",")).map(Long::parseLong).toList();
    }

    private static String emptyIfNull(String value) {
        return value == null ? "" : value;
    }

    private static byte[] bytesIfNull(byte[] value) {
        return value == null ? new byte[0] : value;
    }

    private void clearCaches() {
        worldIds.clear();
        batchIds.clear();
        actorIds.clear();
        stateIds.clear();
    }

    private void closeQuietly() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException ignored) {
            // Preserve the original open failure.
        } finally {
            connection = null;
            clearCaches();
        }
    }

    private static <K, V> Map<K, V> lruCache(int maximumSize) {
        return new LinkedHashMap<>(maximumSize, 0.75F, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return size() > maximumSize;
            }
        };
    }

    @FunctionalInterface
    private interface SqlWork {
        void run() throws SQLException;
    }

    private record StateIdPair(long before, long after) {
    }

    private record MetricDelta(long count, long bytes, long firstOccurredAt, long lastOccurredAt) {
        private MetricDelta merge(MetricDelta other) {
            return new MetricDelta(
                Math.addExact(count, other.count),
                Math.addExact(bytes, other.bytes),
                Math.min(firstOccurredAt, other.firstOccurredAt),
                Math.max(lastOccurredAt, other.lastOccurredAt)
            );
        }
    }
}
