package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.AppliedOperationItem;
import kr.playcity.history.model.OperationCheckpoint;
import kr.playcity.history.model.OperationHeader;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgresHistoryRepositoryTest {
    private static final UUID WORLD_ID = UUID.fromString("50000000-0000-0000-0000-000000000005");
    private static final ActorRef ACTOR = ActorRef.player(
        UUID.fromString("60000000-0000-0000-0000-000000000006"),
        "PostgresBuilder"
    );

    private HistoryConfig.Postgres config;
    private PostgresHistoryRepository repository;

    @BeforeEach
    void openRepositoryWhenPostgresIsConfigured() {
        String host = System.getenv("HISTORY_TEST_POSTGRES_HOST");
        Assumptions.assumeTrue(host != null && !host.isBlank());
        String schema = "history_test_" + UUID.randomUUID().toString().replace("-", "");
        config = new HistoryConfig.Postgres(
            host,
            integerEnvironment("HISTORY_TEST_POSTGRES_PORT", 5432),
            environment("HISTORY_TEST_POSTGRES_DATABASE", "history"),
            schema,
            environment("HISTORY_TEST_POSTGRES_USER", "history"),
            environment("HISTORY_TEST_POSTGRES_PASSWORD", "history-test"),
            environment("HISTORY_TEST_POSTGRES_SSLMODE", "disable"),
            5_000,
            30
        );
        repository = new PostgresHistoryRepository(config);
        repository.open();
    }

    @AfterEach
    void closeAndDropTestSchema() throws Exception {
        if (repository == null) {
            return;
        }
        repository.close();
        Properties properties = new Properties();
        properties.setProperty("user", config.username());
        properties.setProperty("password", config.password());
        properties.setProperty("sslmode", config.sslMode());
        String url = "jdbc:postgresql://" + config.host() + ":" + config.port() + "/" + config.database();
        try (Connection connection = DriverManager.getConnection(url, properties);
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("DROP SCHEMA \"" + config.schema() + "\" CASCADE");
        }
    }

    @Test
    void insertsQueriesFiltersAndPurgesBatches() throws Exception {
        repository.insertBatch(List.of(
            change(100L, 1, "minecraft:stone", "minecraft:dirt"),
            change(200L, 2, "minecraft:dirt", "minecraft:gold_block")
        ));

        List<ChangeRecord> filtered = repository.query(HistoryQuery.nearby(
            WORLD_ID,
            1,
            0,
            10,
            0L,
            "PostgresBuilder",
            null,
            Set.of("gold_block"),
            Set.of(),
            10
        ));

        assertEquals(1, filtered.size());
        assertEquals(2, filtered.getFirst().position().x());
        java.util.Map<BlockPosition, ChangeRecord> latest = repository.latestChanges(List.of(
            new BlockPosition(WORLD_ID, 1, 64, 0),
            new BlockPosition(WORLD_ID, 2, 64, 0)
        ));
        assertEquals(2, latest.size());
        assertEquals(
            "minecraft:gold_block",
            latest.get(new BlockPosition(WORLD_ID, 2, 64, 0)).after().blockData()
        );
        assertEquals(1, repository.purgeChangesBefore(150L, 100));
        assertEquals(1, repository.query(
            HistoryQuery.nearby(WORLD_ID, 1, 0, 10, 0L, null, 10)
        ).size());
        StorageProfile profile = repository.storageProfile();
        assertTrue(profile.databaseBytes() > 0L);
        assertEquals(1, profile.metrics().size());
        assertEquals(2L, profile.metrics().getFirst().changeCount());
        try (Connection connection = inspectionConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO \"" + config.schema() + "\"");
            assertEquals(2L, scalar(statement, "SELECT COUNT(*) FROM block_states"));
        }
    }

    @Test
    void isolatesIdenticalCoordinatesAcrossMultipleWorlds() {
        UUID secondWorld = UUID.fromString("50000000-0000-0000-0000-000000000099");
        repository.insertBatch(List.of(
            change(WORLD_ID, 100L, 4, "minecraft:stone", "minecraft:gold_block"),
            change(secondWorld, 200L, 4, "minecraft:stone", "minecraft:diamond_block")
        ));

        List<ChangeRecord> first = repository.query(
            HistoryQuery.at(WORLD_ID, 4, 64, 0, 0L, 10)
        );
        List<ChangeRecord> second = repository.query(
            HistoryQuery.at(secondWorld, 4, 64, 0, 0L, 10)
        );

        assertEquals(1, first.size());
        assertEquals("minecraft:gold_block", first.getFirst().after().blockData());
        assertEquals(1, second.size());
        assertEquals("minecraft:diamond_block", second.getFirst().after().blockData());
    }

    @Test
    void migratesVersionOneReferenceCounts() throws Exception {
        repository.insertBatch(List.of(change(100L, 3, "minecraft:stone", "minecraft:dirt")));
        repository.close();
        try (Connection connection = inspectionConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO \"" + config.schema() + "\"");
            statement.executeUpdate("ALTER TABLE block_states DROP COLUMN reference_count");
            statement.executeUpdate("UPDATE schema_info SET version = 1 WHERE singleton");
        }

        repository = new PostgresHistoryRepository(config);
        repository.open();

        try (Connection connection = inspectionConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("SET search_path TO \"" + config.schema() + "\"");
            assertEquals(2L, scalar(statement, "SELECT SUM(reference_count) FROM block_states"));
            assertEquals(3L, scalar(statement, "SELECT version FROM schema_info"));
            assertEquals(1L, scalar(statement, "SELECT SUM(change_count) FROM storage_metrics"));
        }
    }

    @Test
    void streamsPendingItemsAndCheckpointsIdempotently() {
        UUID operationId = UUID.randomUUID();
        List<OperationItem> items = new ArrayList<>();
        for (int sequence = 0; sequence < 50; sequence++) {
            items.add(new OperationItem(
                sequence,
                new BlockPosition(WORLD_ID, sequence, 64, 0),
                BlockSnapshot.block("minecraft:stone"),
                BlockSnapshot.block("minecraft:dirt"),
                List.of((long) sequence + 1L)
            ));
        }
        repository.prepareOperation(
            new OperationHeader(operationId, 1_000L, ACTOR, OperationKind.ROLLBACK, "recovery", null, items.size()),
            new OperationItemSource() {
                private int cursor;

                @Override
                public List<OperationItem> readBatch(int maximumItems) {
                    if (cursor == items.size()) {
                        return List.of();
                    }
                    int end = Math.min(items.size(), cursor + maximumItems);
                    List<OperationItem> batch = List.copyOf(items.subList(cursor, end));
                    cursor = end;
                    return batch;
                }

                @Override
                public void close() {
                }
            },
            7
        );
        assertEquals(List.of(operationId), repository.interruptedOperationIds(10));
        List<AppliedOperationItem> first = items.subList(0, 8).stream()
            .map(item -> new AppliedOperationItem(item, item.before(), item.after()))
            .toList();
        OperationCheckpoint checkpoint = new OperationCheckpoint(operationId, 2_000L, first);
        repository.checkpointOperation(checkpoint);
        repository.checkpointOperation(checkpoint);

        List<OperationItem> pending = new ArrayList<>();
        repository.scanPendingOperationItems(operationId, pending::add);
        assertEquals(42, pending.size());
        assertEquals(8, pending.getFirst().sequence());
        assertEquals(49, pending.getLast().sequence());
        assertEquals(8, repository.loadOperationSummary(operationId).orElseThrow().appliedCount());
    }

    private static ChangeRecord change(long time, int x, String before, String after) {
        return change(WORLD_ID, time, x, before, after);
    }

    private static ChangeRecord change(UUID worldId, long time, int x, String before, String after) {
        return new ChangeRecord(
            0L,
            time,
            new BlockPosition(worldId, x, 64, 0),
            ACTOR,
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            "postgres-test"
        );
    }

    private Connection inspectionConnection() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("user", config.username());
        properties.setProperty("password", config.password());
        properties.setProperty("sslmode", config.sslMode());
        String url = "jdbc:postgresql://" + config.host() + ":" + config.port() + "/" + config.database();
        return DriverManager.getConnection(url, properties);
    }

    private static long scalar(Statement statement, String sql) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            result.next();
            return result.getLong(1);
        }
    }

    private static String environment(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int integerEnvironment(String name, int fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : Integer.parseInt(value);
    }
}
