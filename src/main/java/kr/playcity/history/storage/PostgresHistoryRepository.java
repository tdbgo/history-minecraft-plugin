package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

final class PostgresHistoryRepository implements HistoryRepository {
    private static final int SCHEMA_VERSION = 3;
    private static final int STATE_CACHE_SIZE = 16_384;
    private static final int ACTOR_CACHE_SIZE = 4_096;
    private static final int LATEST_POSITION_BATCH_SIZE = 5_000;

    private final HistoryConfig.Postgres config;
    private final Map<UUID, Long> worldIds = lruCache(256);
    private final Map<UUID, Long> batchIds = lruCache(4_096);
    private final Map<ActorRef, Long> actorIds = lruCache(ACTOR_CACHE_SIZE);
    private final Map<BlockSnapshot, Long> stateIds = lruCache(STATE_CACHE_SIZE);
    private Connection connection;

    PostgresHistoryRepository(HistoryConfig.Postgres config) {
        this.config = config;
    }

    @Override
    public void open() {
        if (connection != null) {
            return;
        }
        connectAndMigrate();
    }

    private void connectAndMigrate() {
        closeQuietly();
        try {
            Class.forName("org.postgresql.Driver");
            Properties properties = new Properties();
            properties.setProperty("user", config.username());
            properties.setProperty("password", config.password());
            properties.setProperty("sslmode", config.sslMode());
            properties.setProperty("connectTimeout", Integer.toString(
                Math.max(1, (config.connectTimeoutMillis() + 999) / 1_000)
            ));
            properties.setProperty("socketTimeout", Integer.toString(config.socketTimeoutSeconds()));
            properties.setProperty("ApplicationName", "History");
            properties.setProperty("reWriteBatchedInserts", "true");
            properties.setProperty("tcpKeepAlive", "true");
            String host = config.host().contains(":") ? "[" + config.host() + "]" : config.host();
            String url = "jdbc:postgresql://" + host + ":" + config.port() + "/" + config.database();
            connection = DriverManager.getConnection(url, properties);
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            try (Statement statement = connection.createStatement()) {
                statement.execute("CREATE SCHEMA IF NOT EXISTS " + quotedSchema());
                statement.execute("SET search_path TO " + quotedSchema());
            }
            migrate();
        } catch (ClassNotFoundException | SQLException exception) {
            closeQuietly();
            throw new StorageException("Unable to open the History PostgreSQL database", exception);
        } catch (RuntimeException exception) {
            closeQuietly();
            throw exception;
        }
    }

    private void migrate() throws SQLException {
        inTransaction(() -> {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("""
                    CREATE TABLE IF NOT EXISTS schema_info (
                        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                        version INTEGER NOT NULL
                    )
                    """);
            }
            Integer version = null;
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("SELECT version FROM schema_info WHERE singleton")) {
                if (result.next()) {
                    version = result.getInt(1);
                }
            }
            if (version != null && version > SCHEMA_VERSION) {
                throw new StorageException(
                    "PostgreSQL schema " + version + " is not supported by this History build"
                );
            }
            createSchema();
            if (version == null) {
                try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO schema_info(singleton, version) VALUES (TRUE, ?)"
                )) {
                    statement.setInt(1, SCHEMA_VERSION);
                    statement.executeUpdate();
                }
            } else if (version == 1 || version == 2) {
                if (version == 1) {
                    migrateV1ToV2();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE schema_info SET version = ? WHERE singleton"
                )) {
                    statement.setInt(1, SCHEMA_VERSION);
                    statement.executeUpdate();
                }
            } else if (version != SCHEMA_VERSION) {
                throw new StorageException("No PostgreSQL migration path exists from schema " + version);
            }
        });
        clearCaches();
    }

    private void createSchema() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS worlds (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    uuid UUID NOT NULL UNIQUE
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS actors (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    uuid UUID,
                    name TEXT NOT NULL,
                    kind SMALLINT NOT NULL
                )
                """);
            statement.executeUpdate("""
                CREATE UNIQUE INDEX IF NOT EXISTS actors_uuid_kind
                    ON actors(uuid, kind) WHERE uuid IS NOT NULL
                """);
            statement.executeUpdate("""
                CREATE UNIQUE INDEX IF NOT EXISTS actors_name_kind
                    ON actors(lower(name), kind) WHERE uuid IS NULL
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS block_states (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    fingerprint BYTEA NOT NULL UNIQUE CHECK(octet_length(fingerprint) = 32),
                    block_data TEXT NOT NULL,
                    payload_type TEXT,
                    payload BYTEA,
                    reference_count BIGINT NOT NULL DEFAULT 0 CHECK(reference_count >= 0)
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS edit_batches (
                    id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    uuid UUID NOT NULL UNIQUE
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS changes (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    occurred_at BIGINT NOT NULL,
                    world_id INTEGER NOT NULL REFERENCES worlds(id),
                    chunk_x INTEGER NOT NULL,
                    chunk_z INTEGER NOT NULL,
                    packed_position BIGINT NOT NULL,
                    actor_id INTEGER NOT NULL REFERENCES actors(id),
                    cause SMALLINT NOT NULL,
                    before_state_id INTEGER NOT NULL REFERENCES block_states(id),
                    after_state_id INTEGER NOT NULL REFERENCES block_states(id),
                    operation_uuid UUID,
                    batch_id INTEGER REFERENCES edit_batches(id),
                    metadata TEXT
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS changes_world_chunk_time
                    ON changes(world_id, chunk_x, chunk_z, occurred_at DESC, id DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS changes_world_position_time
                    ON changes(world_id, chunk_x, chunk_z, packed_position, occurred_at DESC, id DESC)
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS changes_actor_time
                    ON changes(actor_id, occurred_at DESC, id DESC)
                """);
            statement.executeUpdate("DROP INDEX IF EXISTS changes_operation");
            statement.executeUpdate("DROP INDEX IF EXISTS changes_batch");
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS operations (
                    operation_uuid UUID PRIMARY KEY,
                    created_at BIGINT NOT NULL,
                    completed_at BIGINT,
                    actor_id INTEGER NOT NULL REFERENCES actors(id),
                    kind SMALLINT NOT NULL,
                    status SMALLINT NOT NULL,
                    summary TEXT NOT NULL,
                    inverse_of UUID,
                    item_count INTEGER NOT NULL,
                    applied_count INTEGER NOT NULL DEFAULT 0,
                    skipped_count INTEGER NOT NULL DEFAULT 0,
                    failure TEXT
                )
                """);
            statement.executeUpdate("""
                CREATE INDEX IF NOT EXISTS operations_actor_time
                    ON operations(actor_id, created_at DESC)
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS operation_items (
                    operation_uuid UUID NOT NULL REFERENCES operations(operation_uuid) ON DELETE CASCADE,
                    sequence INTEGER NOT NULL,
                    world_id INTEGER NOT NULL REFERENCES worlds(id),
                    chunk_x INTEGER NOT NULL,
                    chunk_z INTEGER NOT NULL,
                    packed_position BIGINT NOT NULL,
                    before_state_id INTEGER NOT NULL REFERENCES block_states(id),
                    after_state_id INTEGER NOT NULL REFERENCES block_states(id),
                    source_ids BYTEA,
                    applied BOOLEAN NOT NULL DEFAULT FALSE,
                    PRIMARY KEY (operation_uuid, sequence)
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE IF NOT EXISTS storage_metrics (
                    cause SMALLINT PRIMARY KEY,
                    change_count BIGINT NOT NULL CHECK(change_count >= 0),
                    estimated_input_bytes BIGINT NOT NULL CHECK(estimated_input_bytes >= 0),
                    first_occurred_at BIGINT NOT NULL,
                    last_occurred_at BIGINT NOT NULL
                )
                """);
        }
    }

    private void migrateV1ToV2() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(
                "ALTER TABLE block_states ADD COLUMN reference_count BIGINT NOT NULL DEFAULT 0"
            );
            statement.executeUpdate("""
                CREATE TEMP TABLE history_state_references (
                    id BIGINT PRIMARY KEY,
                    amount BIGINT NOT NULL
                ) ON COMMIT DROP
                """);
            accumulateExistingReferences(statement, "changes", "before_state_id");
            accumulateExistingReferences(statement, "changes", "after_state_id");
            accumulateExistingReferences(statement, "operation_items", "before_state_id");
            accumulateExistingReferences(statement, "operation_items", "after_state_id");
            statement.executeUpdate("""
                UPDATE block_states bs
                   SET reference_count = COALESCE(r.amount, 0)
                  FROM history_state_references r
                 WHERE r.id = bs.id
                """);
        }
    }

    private static void accumulateExistingReferences(Statement statement, String table, String column)
        throws SQLException {
        statement.executeUpdate("""
            INSERT INTO history_state_references(id, amount)
            SELECT %1$s, COUNT(*) FROM %2$s GROUP BY %1$s
            ON CONFLICT(id) DO UPDATE
            SET amount = history_state_references.amount + excluded.amount
            """.formatted(column, table));
    }

    @Override
    public void insertBatch(List<ChangeRecord> changes) {
        if (changes.isEmpty()) {
            return;
        }
        ensureConnected();
        try {
            inTransaction(() -> {
                Map<Long, Integer> references = new LinkedHashMap<>();
                try (PreparedStatement statement = connection.prepareStatement(insertChangeSql())) {
                    for (ChangeRecord change : changes) {
                        addStateReferences(references, bindChange(statement, change));
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                adjustStateReferences(references, 1);
                updateStorageMetrics(metricDeltas(changes));
            });
        } catch (SQLException exception) {
            throw storageFailure("Unable to persist a History change batch to PostgreSQL", exception);
        }
    }

    @Override
    public List<ChangeRecord> query(HistoryQuery query) {
        ensureConnected();
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
            sql.append(" AND ((((c.chunk_x::bigint * 16) + (c.packed_position & 15)) - ?)");
            sql.append(" * (((c.chunk_x::bigint * 16) + (c.packed_position & 15)) - ?)");
            sql.append(" + (((c.chunk_z::bigint * 16) + ((c.packed_position >> 4) & 15)) - ?)");
            sql.append(" * (((c.chunk_z::bigint * 16) + ((c.packed_position >> 4) & 15)) - ?)) <= ?");
        }
        boolean filterActor = query.actor() != null;
        UUID actorUuid = filterActor ? parseUuid(query.actor()) : null;
        if (actorUuid != null) {
            sql.append(" AND a.uuid = ?");
        } else if (filterActor) {
            sql.append(" AND lower(a.name) = lower(?)");
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
                statement.setObject(parameter++, actorUuid);
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
            throw storageFailure("Unable to query History changes from PostgreSQL", exception);
        }
    }

    @Override
    public void scanRollbackChanges(HistoryQuery query, ChangeRecordSink sink) {
        Objects.requireNonNull(query, "query");
        Objects.requireNonNull(sink, "sink");
        if (!query.rollbackOnly() || query.hasCursor()) {
            throw new IllegalArgumentException("Streaming scans require an unpaged rollback query");
        }
        ensureConnected();
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
        appendStreamingSpatialFilter(sql, query);
        boolean filterActor = query.actor() != null;
        UUID actorUuid = filterActor ? parseUuid(query.actor()) : null;
        if (actorUuid != null) {
            sql.append(" AND a.uuid = ?");
        } else if (filterActor) {
            sql.append(" AND lower(a.name) = lower(?)");
        }
        if (query.cause() != null) {
            sql.append(" AND c.cause = ?");
        }
        RollbackCauseFilterSql.append(sql, query);
        MaterialFilterSql.append(sql, query);
        sql.append(" ORDER BY c.chunk_x, c.chunk_z, c.packed_position, c.occurred_at DESC, c.id DESC");

        try {
            inTransaction(() -> {
                try (PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                    statement.setFetchSize(query.limit());
                    int parameter = bindStreamingQueryPrefix(
                        statement,
                        worldId,
                        query,
                        actorUuid,
                        filterActor
                    );
                    RollbackCauseFilterSql.bind(statement, parameter, query);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            sink.accept(readChange(result, query.worldId()));
                        }
                    }
                }
            });
        } catch (SQLException exception) {
            throw storageFailure("Unable to stream PostgreSQL rollback changes", exception);
        }
    }

    private static void appendStreamingSpatialFilter(StringBuilder sql, HistoryQuery query) {
        if (query.exactPosition()) {
            sql.append(" AND c.chunk_x = ? AND c.chunk_z = ? AND c.packed_position = ?");
            return;
        }
        sql.append(" AND c.chunk_x BETWEEN ? AND ? AND c.chunk_z BETWEEN ? AND ?");
        sql.append(" AND ((((c.chunk_x::bigint * 16) + (c.packed_position & 15)) - ?)");
        sql.append(" * (((c.chunk_x::bigint * 16) + (c.packed_position & 15)) - ?)");
        sql.append(" + (((c.chunk_z::bigint * 16) + ((c.packed_position >> 4) & 15)) - ?)");
        sql.append(" * (((c.chunk_z::bigint * 16) + ((c.packed_position >> 4) & 15)) - ?)) <= ?");
    }

    private int bindStreamingQueryPrefix(
        PreparedStatement statement,
        long worldId,
        HistoryQuery query,
        UUID actorUuid,
        boolean filterActor
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
            statement.setObject(parameter++, actorUuid);
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
        ensureConnected();
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
            throw storageFailure("Unable to validate latest PostgreSQL history states", exception);
        }
    }

    @Override
    public void prepareOperation(OperationDraft operation) {
        ensureConnected();
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
                    operationStatement.setObject(1, operation.id());
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
            throw storageFailure("Unable to prepare a History operation in PostgreSQL", exception);
        }
    }

    @Override
    public void prepareOperation(OperationHeader operation, OperationItemSource items, int batchSize) {
        if (batchSize <= 0) {
            throw new IllegalArgumentException("Operation preparation batch size must be positive");
        }
        ensureConnected();
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
                    operationStatement.setObject(1, operation.id());
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
            throw storageFailure("Unable to prepare streamed History operation", exception);
        }
    }

    @Override
    public void checkpointOperation(OperationCheckpoint checkpoint) {
        ensureConnected();
        try {
            inTransaction(() -> checkpointOperationInTransaction(checkpoint));
        } catch (SQLException exception) {
            throw storageFailure("Unable to checkpoint History operation", exception);
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
                UPDATE operation_items SET applied = TRUE
                 WHERE operation_uuid = ? AND sequence = ? AND applied = FALSE
                """)) {
            for (AppliedOperationItem applied : checkpoint.applied()) {
                itemStatement.setObject(1, checkpoint.operationId());
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
                addStateReferences(references, bindChange(changeStatement, auditChange));
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
            statement.setObject(2, checkpoint.operationId());
            statement.setInt(3, OperationStatus.PREPARED.storageCode());
            if (statement.executeUpdate() != 1) {
                throw new StorageException("Prepared operation state changed during checkpoint");
            }
        }
    }

    @Override
    public void finalizeOperation(OperationFinalization finalization) {
        ensureConnected();
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
                    statement.setObject(5, finalization.operationId());
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
            throw storageFailure("Unable to finalize History operation", exception);
        }
    }

    @Override
    public void completeOperation(OperationCompletion completion) {
        ensureConnected();
        try {
            inTransaction(() -> {
                StoredOperation stored = loadOperationInternal(completion.operationId())
                    .orElseThrow(() -> new StorageException("Prepared operation was not found"));
                if (stored.status() != OperationStatus.PREPARED) {
                    throw new StorageException("Operation is no longer in PREPARED state");
                }
                try (PreparedStatement itemStatement = connection.prepareStatement("""
                        UPDATE operation_items SET applied = TRUE
                         WHERE operation_uuid = ? AND sequence = ?
                        """);
                     PreparedStatement changeStatement = connection.prepareStatement(insertChangeSql())) {
                    Map<Long, Integer> references = new LinkedHashMap<>();
                    List<ChangeRecord> auditChanges = new ArrayList<>();
                    ChangeCause cause = stored.draft().kind() == OperationKind.ROLLBACK
                        ? ChangeCause.HISTORY_ROLLBACK
                        : ChangeCause.HISTORY_UNDO;
                    for (AppliedOperationItem applied : completion.applied()) {
                        itemStatement.setObject(1, completion.operationId());
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
                        addStateReferences(references, bindChange(changeStatement, auditChange));
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
                    statement.setObject(6, completion.operationId());
                    statement.setInt(7, OperationStatus.PREPARED.storageCode());
                    if (statement.executeUpdate() != 1) {
                        throw new StorageException("Prepared operation state changed during completion");
                    }
                }
            });
        } catch (SQLException exception) {
            throw storageFailure("Unable to complete a History operation in PostgreSQL", exception);
        }
    }

    @Override
    public Optional<StoredOperation> loadOperation(UUID operationId) {
        ensureConnected();
        try {
            return loadOperationInternal(operationId);
        } catch (SQLException exception) {
            throw storageFailure("Unable to load a History operation from PostgreSQL", exception);
        }
    }

    @Override
    public Optional<StoredOperation> findLastOperation(UUID actorId) {
        ensureConnected();
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT o.operation_uuid
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE a.uuid = ? AND o.status IN (?, ?)
             ORDER BY o.created_at DESC
             LIMIT 1
            """)) {
            statement.setObject(1, actorId);
            statement.setInt(2, OperationStatus.APPLIED.storageCode());
            statement.setInt(3, OperationStatus.PARTIAL.storageCode());
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                    ? loadOperationInternal(result.getObject(1, UUID.class))
                    : Optional.empty();
            }
        } catch (SQLException exception) {
            throw storageFailure("Unable to find the last History operation in PostgreSQL", exception);
        }
    }

    @Override
    public Optional<OperationSummary> loadOperationSummary(UUID operationId) {
        ensureConnected();
        try {
            return loadOperationSummaryInternal(operationId);
        } catch (SQLException exception) {
            throw storageFailure("Unable to load History operation summary", exception);
        }
    }

    @Override
    public Optional<OperationSummary> findLastOperationSummary(UUID actorId) {
        ensureConnected();
        String sql = """
            SELECT o.operation_uuid
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE a.uuid = ? AND o.status IN (?, ?)
             ORDER BY o.created_at DESC
             LIMIT 1
            """;
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setObject(1, actorId);
            statement.setInt(2, OperationStatus.APPLIED.storageCode());
            statement.setInt(3, OperationStatus.PARTIAL.storageCode());
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                    ? loadOperationSummaryInternal(result.getObject(1, UUID.class))
                    : Optional.empty();
            }
        } catch (SQLException exception) {
            throw storageFailure("Unable to find the last History operation summary", exception);
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
        ensureConnected();
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
        try {
            inTransaction(() -> {
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setObject(1, operationId);
                    statement.setBoolean(2, applied);
                    statement.setFetchSize(2_048);
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next()) {
                            sink.accept(readOperationItem(result));
                        }
                    }
                }
            });
        } catch (SQLException exception) {
            throw storageFailure("Unable to stream History operation items", exception);
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
            statement.setObject(1, operationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                ActorRef actor = new ActorRef(
                    result.getObject("actor_uuid", UUID.class),
                    result.getString("actor_name"),
                    ActorKind.fromStorageCode(result.getInt("actor_kind"))
                );
                OperationHeader header = new OperationHeader(
                    operationId,
                    result.getLong("created_at"),
                    actor,
                    OperationKind.fromStorageCode(result.getInt("kind")),
                    result.getString("summary"),
                    result.getObject("inverse_of", UUID.class),
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
        UUID worldId = result.getObject("world_uuid", UUID.class);
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
            statement.setObject(1, operationId);
            statement.setInt(2, sequence);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        } catch (SQLException exception) {
            throw storageFailure("Unable to verify a History operation checkpoint", exception);
        }
    }

    @Override
    public int interruptedOperationCount() {
        ensureConnected();
        try (PreparedStatement statement = connection.prepareStatement(
            "SELECT COUNT(*) FROM operations WHERE status = ?"
        )) {
            statement.setInt(1, OperationStatus.PREPARED.storageCode());
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getInt(1) : 0;
            }
        } catch (SQLException exception) {
            throw storageFailure("Unable to count interrupted History operations in PostgreSQL", exception);
        }
    }

    @Override
    public List<UUID> interruptedOperationIds(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        ensureConnected();
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
                    ids.add(result.getObject(1, UUID.class));
                }
            }
            return List.copyOf(ids);
        } catch (SQLException exception) {
            throw storageFailure("Unable to list interrupted History operations in PostgreSQL", exception);
        }
    }

    @Override
    public int purgeChangesBefore(long cutoffMillis, int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        ensureConnected();
        try {
            int[] deleted = new int[1];
            inTransaction(() -> {
                Map<Long, Integer> references = new LinkedHashMap<>();
                try (PreparedStatement statement = connection.prepareStatement("""
                    WITH doomed AS (
                        SELECT id FROM changes
                         WHERE occurred_at < ?
                         ORDER BY id
                         LIMIT ?
                    )
                    DELETE FROM changes c USING doomed d
                     WHERE c.id = d.id
                    RETURNING c.before_state_id, c.after_state_id
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
            }
            return deleted[0];
        } catch (SQLException exception) {
            throw storageFailure("Unable to purge expired History changes from PostgreSQL", exception);
        }
    }

    @Override
    public StorageProfile storageProfile() {
        ensureConnected();
        try {
            long databaseBytes = 0L;
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery("""
                     SELECT COALESCE(SUM(pg_total_relation_size(c.oid)), 0)
                       FROM pg_class c
                       JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname = current_schema() AND c.relkind = 'r'
                     """)) {
                if (result.next()) {
                    databaseBytes = result.getLong(1);
                }
            }
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
            return new StorageProfile("PostgreSQL", databaseBytes, 0L, metrics);
        } catch (SQLException exception) {
            throw storageFailure("Unable to profile History PostgreSQL storage", exception);
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
    public String backendName() {
        return "PostgreSQL";
    }

    @Override
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (SQLException exception) {
            throw new StorageException("Unable to close the History PostgreSQL database", exception);
        } finally {
            connection = null;
            clearCaches();
        }
    }

    private Optional<StoredOperation> loadOperationInternal(UUID operationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("""
            SELECT o.created_at, a.uuid AS actor_uuid, a.name AS actor_name,
                   a.kind AS actor_kind, o.kind, o.status, o.summary, o.inverse_of,
                   o.applied_count, o.skipped_count, o.failure
              FROM operations o
              JOIN actors a ON a.id = o.actor_id
             WHERE o.operation_uuid = ?
            """)) {
            statement.setObject(1, operationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return Optional.empty();
                }
                ActorRef actor = new ActorRef(
                    result.getObject("actor_uuid", UUID.class),
                    result.getString("actor_name"),
                    ActorKind.fromStorageCode(result.getInt("actor_kind"))
                );
                OperationKind kind = OperationKind.fromStorageCode(result.getInt("kind"));
                OperationStatus status = OperationStatus.fromStorageCode(result.getInt("status"));
                List<OperationItem> items = loadOperationItems(operationId, status);
                OperationDraft draft = new OperationDraft(
                    operationId,
                    result.getLong("created_at"),
                    actor,
                    kind,
                    result.getString("summary"),
                    result.getObject("inverse_of", UUID.class),
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
        try (PreparedStatement statement = connection.prepareStatement("""
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
            """)) {
            statement.setObject(1, operationId);
            try (ResultSet result = statement.executeQuery()) {
                List<OperationItem> items = new ArrayList<>();
                while (result.next()) {
                    boolean appliedOnly = status == OperationStatus.APPLIED || status == OperationStatus.PARTIAL;
                    if (appliedOnly && !result.getBoolean("applied")) {
                        continue;
                    }
                    UUID worldId = result.getObject("world_uuid", UUID.class);
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

    private static String insertChangeSql() {
        return """
            INSERT INTO changes(
                occurred_at, world_id, chunk_x, chunk_z, packed_position,
                actor_id, cause, before_state_id, after_state_id,
                operation_uuid, batch_id, metadata
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    }

    private static String insertOperationItemSql() {
        return """
            INSERT INTO operation_items(
                operation_uuid, sequence, world_id, chunk_x, chunk_z, packed_position,
                before_state_id, after_state_id, source_ids, applied
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;
    }

    private StateIdPair bindChange(PreparedStatement statement, ChangeRecord change) throws SQLException {
        int parameter = 1;
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
        statement.setObject(parameter++, operationId);
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
            statement.setNull(parameter++, Types.BINARY);
        } else {
            statement.setBytes(parameter++, sourceIds);
        }
        statement.setBoolean(parameter, applied);
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
                change_count = storage_metrics.change_count + excluded.change_count,
                estimated_input_bytes = storage_metrics.estimated_input_bytes
                    + excluded.estimated_input_bytes,
                first_occurred_at = LEAST(storage_metrics.first_occurred_at, excluded.first_occurred_at),
                last_occurred_at = GREATEST(storage_metrics.last_occurred_at, excluded.last_occurred_at)
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
                    throw new StorageException("A referenced PostgreSQL block state was not found");
                }
            }
        }
    }

    private ChangeRecord readChange(ResultSet result, UUID worldId) throws SQLException {
        ActorRef actor = new ActorRef(
            result.getObject("actor_uuid", UUID.class),
            result.getString("actor_name"),
            ActorKind.fromStorageCode(result.getInt("actor_kind"))
        );
        return new ChangeRecord(
            result.getLong("id"),
            result.getLong("occurred_at"),
            PositionCodec.unpack(
                worldId,
                result.getInt("chunk_x"),
                result.getInt("chunk_z"),
                result.getLong("packed_position")
            ),
            actor,
            ChangeCause.fromStorageCode(result.getInt("cause")),
            readSnapshot(result, "before"),
            readSnapshot(result, "after"),
            result.getObject("operation_uuid", UUID.class),
            result.getObject("batch_uuid", UUID.class),
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

    private long worldId(UUID worldUuid) throws SQLException {
        Long cached = worldIds.get(worldUuid);
        if (cached != null) {
            return cached;
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO worlds(uuid) VALUES (?)
                ON CONFLICT DO NOTHING
                RETURNING id
                """)) {
            insert.setObject(1, worldUuid);
            try (ResultSet result = insert.executeQuery()) {
                if (result.next()) {
                    long id = result.getLong(1);
                    worldIds.put(worldUuid, id);
                    return id;
                }
            }
        }
        Long existing = findWorldId(worldUuid);
        if (existing == null) {
            throw new StorageException("PostgreSQL did not resolve the History world ID");
        }
        return existing;
    }

    private Long findWorldId(UUID worldUuid) {
        Long cached = worldIds.get(worldUuid);
        if (cached != null) {
            return cached;
        }
        try (PreparedStatement statement = connection.prepareStatement("SELECT id FROM worlds WHERE uuid = ?")) {
            statement.setObject(1, worldUuid);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                long id = result.getLong(1);
                worldIds.put(worldUuid, id);
                return id;
            }
        } catch (SQLException exception) {
            throw storageFailure("Unable to resolve a History world in PostgreSQL", exception);
        }
    }

    private long actorId(ActorRef actor) throws SQLException {
        Long cached = actorIds.get(actor);
        if (cached != null) {
            return cached;
        }
        Long existing = findActorId(actor);
        if (existing != null) {
            actorIds.put(actor, existing);
            return existing;
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO actors(uuid, name, kind) VALUES (?, ?, ?)
                ON CONFLICT DO NOTHING
                RETURNING id
                """)) {
            setNullableUuid(insert, 1, actor.uuid());
            insert.setString(2, actor.name());
            insert.setInt(3, actor.kind().storageCode());
            try (ResultSet result = insert.executeQuery()) {
                if (result.next()) {
                    long id = result.getLong(1);
                    actorIds.put(actor, id);
                    return id;
                }
            }
        }
        existing = findActorId(actor);
        if (existing == null) {
            throw new StorageException("PostgreSQL did not resolve the History actor ID");
        }
        actorIds.put(actor, existing);
        return existing;
    }

    private Long findActorId(ActorRef actor) throws SQLException {
        String sql = actor.uuid() == null
            ? "SELECT id, name FROM actors WHERE uuid IS NULL AND lower(name) = lower(?) AND kind = ?"
            : "SELECT id, name FROM actors WHERE uuid = ? AND kind = ?";
        try (PreparedStatement select = connection.prepareStatement(sql)) {
            if (actor.uuid() == null) {
                select.setString(1, actor.name());
            } else {
                select.setObject(1, actor.uuid());
            }
            select.setInt(2, actor.kind().storageCode());
            try (ResultSet result = select.executeQuery()) {
                if (!result.next()) {
                    return null;
                }
                long id = result.getLong("id");
                if (actor.uuid() != null && !result.getString("name").equals(actor.name())) {
                    try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE actors SET name = ? WHERE id = ?"
                    )) {
                        update.setString(1, actor.name());
                        update.setLong(2, id);
                        update.executeUpdate();
                    }
                }
                return id;
            }
        }
    }

    private long stateId(BlockSnapshot snapshot) throws SQLException {
        Long cached = stateIds.get(snapshot);
        if (cached != null) {
            return cached;
        }
        byte[] fingerprint = fingerprint(snapshot);
        StoredSnapshotCodec.Encoded encoded = StoredSnapshotCodec.encode(snapshot);
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO block_states(fingerprint, block_data, payload_type, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT DO NOTHING
                RETURNING id
                """)) {
            insert.setBytes(1, fingerprint);
            insert.setString(2, snapshot.blockData());
            if (encoded.payloadType().isEmpty()) {
                insert.setNull(3, Types.VARCHAR);
                insert.setNull(4, Types.BINARY);
            } else {
                insert.setString(3, encoded.payloadType());
                insert.setBytes(4, encoded.payload());
            }
            try (ResultSet result = insert.executeQuery()) {
                if (result.next()) {
                    long id = result.getLong(1);
                    stateIds.put(snapshot, id);
                    return id;
                }
            }
        }
        try (PreparedStatement select = connection.prepareStatement("""
            SELECT id, block_data, payload_type, payload FROM block_states WHERE fingerprint = ?
            """)) {
            select.setBytes(1, fingerprint);
            try (ResultSet result = select.executeQuery()) {
                if (!result.next()) {
                    throw new StorageException("PostgreSQL did not resolve the History block-state ID");
                }
                BlockSnapshot stored = StoredSnapshotCodec.decode(
                    result.getString("block_data"),
                    emptyIfNull(result.getString("payload_type")),
                    bytesIfNull(result.getBytes("payload"))
                );
                if (!stored.equals(snapshot)) {
                    throw new StorageException("A block-state fingerprint collision was detected");
                }
                long id = result.getLong("id");
                stateIds.put(snapshot, id);
                return id;
            }
        }
    }

    private long batchId(UUID batchUuid) throws SQLException {
        Long cached = batchIds.get(batchUuid);
        if (cached != null) {
            return cached;
        }
        try (PreparedStatement insert = connection.prepareStatement("""
                INSERT INTO edit_batches(uuid) VALUES (?)
                ON CONFLICT DO NOTHING
                RETURNING id
                """)) {
            insert.setObject(1, batchUuid);
            try (ResultSet result = insert.executeQuery()) {
                if (result.next()) {
                    long id = result.getLong(1);
                    batchIds.put(batchUuid, id);
                    return id;
                }
            }
        }
        try (PreparedStatement select = connection.prepareStatement("SELECT id FROM edit_batches WHERE uuid = ?")) {
            select.setObject(1, batchUuid);
            try (ResultSet result = select.executeQuery()) {
                if (!result.next()) {
                    throw new StorageException("PostgreSQL did not resolve the History batch ID");
                }
                long id = result.getLong(1);
                batchIds.put(batchUuid, id);
                return id;
            }
        }
    }

    private void ensureConnected() {
        try {
            if (connection == null || connection.isClosed()) {
                connectAndMigrate();
            }
        } catch (SQLException exception) {
            throw storageFailure("Unable to validate the History PostgreSQL connection", exception);
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

    private StorageException storageFailure(String message, SQLException exception) {
        String state = exception.getSQLState();
        if (state != null && state.startsWith("08")) {
            closeQuietly();
        }
        return new StorageException(message, exception);
    }

    private void setNullableBatchId(PreparedStatement statement, int parameter, UUID value)
        throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.INTEGER);
        } else {
            statement.setLong(parameter, batchId(value));
        }
    }

    private static void setNullableUuid(PreparedStatement statement, int parameter, UUID value)
        throws SQLException {
        if (value == null) {
            statement.setNull(parameter, Types.OTHER);
        } else {
            statement.setObject(parameter, value);
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

    private String quotedSchema() {
        return "\"" + config.schema() + "\"";
    }

    private void clearCaches() {
        worldIds.clear();
        batchIds.clear();
        actorIds.clear();
        stateIds.clear();
    }

    private void closeQuietly() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException ignored) {
                // Preserve the original connection failure.
            }
        }
        connection = null;
        clearCaches();
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

    private static UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private static String emptyIfNull(String value) {
        return value == null ? "" : value;
    }

    private static byte[] bytesIfNull(byte[] value) {
        return value == null ? new byte[0] : value;
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
