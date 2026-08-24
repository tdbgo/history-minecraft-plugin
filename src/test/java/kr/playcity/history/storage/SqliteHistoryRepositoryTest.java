package kr.playcity.history.storage;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqliteHistoryRepositoryTest {
    private static final UUID WORLD_ID = UUID.fromString("30000000-0000-0000-0000-000000000003");
    private static final UUID ACTOR_ID = UUID.fromString("40000000-0000-0000-0000-000000000004");
    private static final ActorRef ACTOR = ActorRef.player(ACTOR_ID, "Builder");

    @TempDir
    Path temporaryDirectory;

    private SqliteHistoryRepository repository;

    @BeforeEach
    void openRepository() {
        repository = new SqliteHistoryRepository(temporaryDirectory.resolve("history.db"), 1_000);
        repository.open();
    }

    @AfterEach
    void closeRepository() {
        repository.close();
    }

    @Test
    void storesAndFiltersChangesByPositionAndActor() {
        ChangeRecord first = change(100L, 10, 64, 20, "minecraft:stone", "minecraft:dirt");
        ChangeRecord second = change(200L, 11, 64, 20, "minecraft:dirt", "minecraft:gold_block");
        repository.insertBatch(List.of(first, second));

        List<ChangeRecord> exact = repository.query(HistoryQuery.at(WORLD_ID, 10, 64, 20, 0L, 10));
        List<ChangeRecord> nearby = repository.query(HistoryQuery.nearby(
            WORLD_ID, 10, 20, 2, 0L, "builder", 10
        ));

        assertEquals(1, exact.size());
        assertEquals("minecraft:dirt", exact.getFirst().after().blockData());
        assertEquals(2, nearby.size());
        assertTrue(nearby.getFirst().id() > 0L);
        StorageProfile profile = repository.storageProfile();
        assertTrue(profile.totalBytes() > 0L);
        assertEquals(1, profile.metrics().size());
        assertEquals(ChangeCause.PLAYER_PLACE, profile.metrics().getFirst().cause());
        assertEquals(2L, profile.metrics().getFirst().changeCount());
        assertTrue(profile.metrics().getFirst().estimatedBytesPerChange() > 0.0D);
    }

    @Test
    void keysetCursorReturnsEveryRecordWithoutDuplicatesAtEqualTimestamps() {
        List<ChangeRecord> inserted = new ArrayList<>();
        for (int index = 0; index < 5; index++) {
            inserted.add(change(500L, index, 64, 0, "minecraft:stone", "minecraft:dirt"));
        }
        repository.insertBatch(inserted);

        HistoryQuery base = HistoryQuery.nearby(WORLD_ID, 2, 0, 10, 0L, null, 2);
        List<ChangeRecord> first = repository.query(base);
        List<ChangeRecord> second = repository.query(base.before(first.getLast()));
        List<ChangeRecord> third = repository.query(base.before(second.getLast()));
        List<Long> ids = new ArrayList<>();
        first.forEach(change -> ids.add(change.id()));
        second.forEach(change -> ids.add(change.id()));
        third.forEach(change -> ids.add(change.id()));

        assertEquals(5, ids.size());
        assertEquals(5, ids.stream().distinct().count());
    }

    @Test
    void latestStateLookupUsesExactPositionsAndIgnoresAuditOnlyRows() {
        BlockPosition position = new BlockPosition(WORLD_ID, 10, 64, 20);
        ChangeRecord older = change(100L, 10, 64, 20, "minecraft:stone", "minecraft:dirt");
        ChangeRecord newer = new ChangeRecord(
            0L,
            200L,
            position,
            ACTOR,
            ChangeCause.WORLD_EDIT,
            BlockSnapshot.block("minecraft:dirt"),
            BlockSnapshot.block("minecraft:gold_block"),
            null,
            UUID.randomUUID(),
            ""
        );
        ChangeRecord audit = new ChangeRecord(
            0L,
            300L,
            position,
            ACTOR,
            ChangeCause.PLAYER_COMMAND,
            BlockSnapshot.air(),
            BlockSnapshot.air(),
            null,
            "command:/test"
        );
        repository.insertBatch(List.of(older, newer, audit));

        java.util.Map<BlockPosition, ChangeRecord> latest = repository.latestChanges(List.of(
            position,
            new BlockPosition(WORLD_ID, 999, 64, 999)
        ));

        assertEquals(1, latest.size());
        assertEquals("minecraft:gold_block", latest.get(position).after().blockData());
        assertEquals(ChangeCause.WORLD_EDIT, latest.get(position).cause());
        assertThrows(IllegalArgumentException.class, () ->
            repository.latestChanges(List.of(position, position)));
    }

    @Test
    void filtersByIncludedAndExcludedMaterialAcrossBeforeAndAfterStates() {
        repository.insertBatch(List.of(
            change(100L, 1, 64, 1, "minecraft:stone[axis=y]", "minecraft:dirt"),
            change(200L, 2, 64, 1, "minecraft:dirt", "minecraft:gold_block"),
            change(300L, 3, 64, 1, "minecraft:oak_log[axis=x]", "minecraft:oak_planks")
        ));

        HistoryQuery query = HistoryQuery.nearby(
            WORLD_ID,
            2,
            1,
            10,
            0L,
            null,
            null,
            Set.of("stone", "gold_block"),
            Set.of("dirt"),
            10
        );

        assertTrue(repository.query(query).isEmpty());
        List<ChangeRecord> included = repository.query(HistoryQuery.nearby(
            WORLD_ID,
            2,
            1,
            10,
            0L,
            null,
            null,
            Set.of("stone"),
            Set.of(),
            10
        ));
        assertEquals(1, included.size());
        assertEquals(1, included.getFirst().position().x());
    }

    @Test
    void compressesLargePayloadsTransparently() throws Exception {
        byte[] beforePayload = new byte[16_384];
        byte[] afterPayload = new byte[16_384];
        java.util.Arrays.fill(beforePayload, (byte) 7);
        java.util.Arrays.fill(afterPayload, (byte) 9);
        ChangeRecord record = new ChangeRecord(
            0L,
            100L,
            new BlockPosition(WORLD_ID, 8, 70, 8),
            ACTOR,
            ChangeCause.CONTAINER,
            new BlockSnapshot("minecraft:chest", "inventory/v1", beforePayload),
            new BlockSnapshot("minecraft:chest", "inventory/v1", afterPayload),
            null,
            "container"
        );
        repository.insertBatch(List.of(record));

        ChangeRecord restored = repository.query(HistoryQuery.at(WORLD_ID, 8, 70, 8, 0L, 10)).getFirst();
        assertEquals(record.before(), restored.before());
        assertEquals(record.after(), restored.after());
        try (Connection inspection = DriverManager.getConnection(
            "jdbc:sqlite:" + temporaryDirectory.resolve("history.db").toAbsolutePath()
        ); Statement statement = inspection.createStatement();
             ResultSet result = statement.executeQuery(
                 "SELECT payload_type, length(payload) FROM block_states WHERE payload IS NOT NULL LIMIT 1"
             )) {
            assertTrue(result.next());
            assertTrue(result.getString(1).startsWith("deflate/"));
            assertTrue(result.getInt(2) < 1_000);
        }
    }

    @Test
    void rollbackQueriesExcludeAuditOnlyEvents() {
        ChangeRecord block = change(100L, 4, 64, 4, "minecraft:stone", "minecraft:dirt");
        ChangeRecord session = new ChangeRecord(
            0L,
            200L,
            new BlockPosition(WORLD_ID, 4, 64, 4),
            ACTOR,
            ChangeCause.PLAYER_SESSION,
            BlockSnapshot.air(),
            BlockSnapshot.air(),
            null,
            "join"
        );
        repository.insertBatch(List.of(block, session));

        List<ChangeRecord> changes = repository.query(
            HistoryQuery.nearby(WORLD_ID, 4, 4, 1, 0L, null, 10).forRollback()
        );

        assertEquals(1, changes.size());
        assertEquals(ChangeCause.PLAYER_PLACE, changes.getFirst().cause());
    }

    @Test
    void retentionPurgeReclaimsUniquePayloadDictionaryRows() throws Exception {
        byte[] beforePayload = new byte[2_048];
        byte[] afterPayload = new byte[2_048];
        java.util.Arrays.fill(beforePayload, (byte) 1);
        java.util.Arrays.fill(afterPayload, (byte) 2);
        repository.insertBatch(List.of(new ChangeRecord(
            0L,
            100L,
            new BlockPosition(WORLD_ID, 9, 70, 9),
            ACTOR,
            ChangeCause.CONTAINER,
            new BlockSnapshot("minecraft:chest", "inventory/v1", beforePayload),
            new BlockSnapshot("minecraft:chest", "inventory/v1", afterPayload),
            null,
            "container"
        )));

        assertEquals(1, repository.purgeChangesBefore(101L, 100));
        try (Connection inspection = DriverManager.getConnection(
            "jdbc:sqlite:" + temporaryDirectory.resolve("history.db").toAbsolutePath()
        ); Statement statement = inspection.createStatement()) {
            assertEquals(0L, scalar(statement, "SELECT COUNT(*) FROM changes"));
            assertEquals(0L, scalar(statement, "SELECT COUNT(*) FROM block_states"));
        }
    }

    @Test
    void migratesVersionTwoReferenceCounts() throws Exception {
        repository.close();
        Path versionTwoFile = temporaryDirectory.resolve("version-two.db");
        repository = new SqliteHistoryRepository(versionTwoFile, 1_000);
        repository.open();
        repository.insertBatch(List.of(change(
            100L, 10, 64, 10, "minecraft:stone", "minecraft:dirt"
        )));
        repository.close();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + versionTwoFile.toAbsolutePath());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("ALTER TABLE block_states DROP COLUMN reference_count");
            statement.executeUpdate("PRAGMA user_version=2");
        }

        repository = new SqliteHistoryRepository(versionTwoFile, 1_000);
        repository.open();

        try (Connection inspection = DriverManager.getConnection("jdbc:sqlite:" + versionTwoFile.toAbsolutePath());
             Statement statement = inspection.createStatement()) {
            assertEquals(4L, scalar(statement, "PRAGMA user_version"));
            assertEquals(2L, scalar(statement, "SELECT SUM(reference_count) FROM block_states"));
            assertEquals(1L, scalar(statement, "SELECT COUNT(*) FROM storage_metrics"));
        }
    }

    @Test
    void deduplicatesRepeatedDimensionsActorsAndBlockStatesWithinSizeBudget() throws Exception {
        List<ChangeRecord> changes = new ArrayList<>(10_000);
        UUID batchId = UUID.randomUUID();
        for (int index = 0; index < 10_000; index++) {
            changes.add(batchedChange(
                1_000L + index,
                index & 255,
                64 + index % 4,
                (index >>> 8) & 255,
                "minecraft:stone",
                "minecraft:dirt",
                batchId
            ));
        }
        repository.insertBatch(changes);

        List<ChangeRecord> stored = repository.query(HistoryQuery.at(WORLD_ID, 0, 64, 0, 0L, 1));
        assertEquals(batchId, stored.getFirst().batchId());

        try (Connection inspection = DriverManager.getConnection(
            "jdbc:sqlite:" + temporaryDirectory.resolve("history.db").toAbsolutePath()
        ); Statement statement = inspection.createStatement()) {
            assertEquals(1L, scalar(statement, "SELECT COUNT(*) FROM worlds"));
            assertEquals(1L, scalar(statement, "SELECT COUNT(*) FROM actors"));
            assertEquals(2L, scalar(statement, "SELECT COUNT(*) FROM block_states"));
            assertEquals(1L, scalar(statement, "SELECT COUNT(*) FROM edit_batches"));
            assertEquals(10_000L, scalar(statement, "SELECT COUNT(*) FROM changes"));
            assertEquals(0L, scalar(statement, """
                SELECT COUNT(*) FROM sqlite_master
                 WHERE type = 'index' AND name IN ('changes_operation', 'changes_batch')
                """));
        }

        repository.close();
        long databaseBytes = Files.size(temporaryDirectory.resolve("history.db"));
        assertTrue(databaseBytes < 4_000_000L, "database bytes: " + databaseBytes);
    }

    @Test
    void migratesVersionOneRowsIntoTheNormalizedBinarySchema() throws Exception {
        repository.close();
        Path legacyFile = temporaryDirectory.resolve("legacy.db");
        createLegacyDatabase(legacyFile);

        repository = new SqliteHistoryRepository(legacyFile, 1_000);
        repository.open();

        List<ChangeRecord> migrated = repository.query(HistoryQuery.at(WORLD_ID, 4, 70, -8, 0L, 10));
        assertEquals(1, migrated.size());
        assertEquals(7L, migrated.getFirst().id());
        assertEquals("minecraft:oak_planks", migrated.getFirst().after().blockData());
        try (Connection inspection = DriverManager.getConnection("jdbc:sqlite:" + legacyFile.toAbsolutePath());
             Statement statement = inspection.createStatement()) {
            assertEquals(4L, scalar(statement, "PRAGMA user_version"));
            assertEquals("blob", textScalar(statement, "SELECT typeof(uuid) FROM worlds LIMIT 1"));
            assertEquals(0L, scalar(statement, "SELECT COUNT(*) FROM storage_metrics"));
        }
    }

    @Test
    void preparesCompletesAndReloadsAuditableOperations() {
        BlockPosition position = new BlockPosition(WORLD_ID, 5, 70, 7);
        OperationItem item = new OperationItem(
            0,
            position,
            BlockSnapshot.block("minecraft:gold_block"),
            BlockSnapshot.block("minecraft:stone"),
            List.of(42L)
        );
        UUID operationId = UUID.randomUUID();
        OperationDraft draft = new OperationDraft(
            operationId,
            500L,
            ACTOR,
            OperationKind.ROLLBACK,
            "test rollback",
            null,
            List.of(item)
        );

        repository.prepareOperation(draft);
        assertEquals(1, repository.interruptedOperationCount());
        assertEquals(OperationStatus.PREPARED, repository.loadOperation(operationId).orElseThrow().status());

        repository.completeOperation(new OperationCompletion(
            operationId,
            600L,
            OperationStatus.APPLIED,
            List.of(new AppliedOperationItem(item, item.before(), item.after())),
            0,
            ""
        ));

        assertEquals(0, repository.interruptedOperationCount());
        assertEquals(OperationStatus.APPLIED, repository.loadOperation(operationId).orElseThrow().status());
        assertEquals(operationId, repository.findLastOperation(ACTOR_ID).orElseThrow().draft().id());
        List<ChangeRecord> audit = repository.query(HistoryQuery.at(WORLD_ID, 5, 70, 7, 0L, 10));
        assertEquals(1, audit.size());
        assertEquals(ChangeCause.HISTORY_ROLLBACK, audit.getFirst().cause());
        assertEquals(operationId, audit.getFirst().operationId());
    }

    @Test
    void streamsRollbackQueriesWithoutTreatingFetchSizeAsAResultLimit() {
        int changeCount = 12_000;
        List<ChangeRecord> changes = new ArrayList<>(changeCount);
        for (int index = 0; index < changeCount; index++) {
            changes.add(change(
                1_000L + index,
                index,
                64,
                0,
                "minecraft:stone",
                "minecraft:dirt"
            ));
        }
        repository.insertBatch(changes);
        List<Integer> streamedX = new ArrayList<>();

        repository.scanRollbackChanges(
            HistoryQuery.nearby(WORLD_ID, changeCount / 2, 0, changeCount, 0L, null, 127)
                .forRollback(),
            change -> streamedX.add(change.position().x())
        );

        assertEquals(changeCount, streamedX.size());
        assertEquals(0, streamedX.getFirst());
        assertEquals(changeCount - 1, streamedX.getLast());
    }

    @Test
    void streamsOperationPreparationAndCheckpointsEachChunkIdempotently() {
        int itemCount = 5_000;
        UUID operationId = UUID.randomUUID();
        OperationHeader header = new OperationHeader(
            operationId,
            1_000L,
            ACTOR,
            OperationKind.ROLLBACK,
            "large streamed rollback",
            null,
            itemCount
        );
        repository.prepareOperation(header, new GeneratedOperationSource(itemCount), 73);
        assertEquals(OperationStatus.PREPARED, repository.loadOperationSummary(operationId).orElseThrow().status());
        assertEquals(List.of(operationId), repository.interruptedOperationIds(10));
        assertEquals(itemCount, repository.loadOperationSummary(operationId).orElseThrow().header().itemCount());

        List<AppliedOperationItem> firstChunk = new ArrayList<>();
        for (int sequence = 0; sequence < 16; sequence++) {
            OperationItem item = generatedItem(sequence);
            firstChunk.add(new AppliedOperationItem(item, item.before(), item.after()));
        }
        OperationCheckpoint checkpoint = new OperationCheckpoint(operationId, 2_000L, firstChunk);
        repository.checkpointOperation(checkpoint);
        repository.checkpointOperation(checkpoint);

        assertEquals(16, repository.loadOperationSummary(operationId).orElseThrow().appliedCount());
        List<OperationItem> applied = new ArrayList<>();
        repository.scanAppliedOperationItems(operationId, applied::add);
        assertEquals(16, applied.size());
        assertEquals(0, applied.getFirst().sequence());
        assertEquals(15, applied.getLast().sequence());
        List<OperationItem> pending = new ArrayList<>();
        repository.scanPendingOperationItems(operationId, pending::add);
        assertEquals(itemCount - firstChunk.size(), pending.size());
        assertEquals(16, pending.getFirst().sequence());
        assertEquals(itemCount - 1, pending.getLast().sequence());

        // A recovery retry checkpoints the same chunk without double counting.
        repository.checkpointOperation(checkpoint);
        assertEquals(16, repository.loadOperationSummary(operationId).orElseThrow().appliedCount());

        repository.finalizeOperation(new OperationFinalization(
            operationId,
            3_000L,
            OperationStatus.PARTIAL,
            itemCount - firstChunk.size(),
            "test-stop"
        ));
        repository.finalizeOperation(new OperationFinalization(
            operationId,
            3_000L,
            OperationStatus.PARTIAL,
            itemCount - firstChunk.size(),
            "test-stop"
        ));
        assertEquals(OperationStatus.PARTIAL, repository.loadOperationSummary(operationId).orElseThrow().status());
        assertEquals(0, repository.interruptedOperationCount());
        assertTrue(repository.interruptedOperationIds(10).isEmpty());
        assertEquals(
            16,
            repository.query(HistoryQuery.nearby(WORLD_ID, 8, 0, 32, 0L, null, 100)).size()
        );
    }

    private static OperationItem generatedItem(int sequence) {
        return new OperationItem(
            sequence,
            new BlockPosition(WORLD_ID, sequence, 64, 0),
            BlockSnapshot.block("minecraft:dirt"),
            BlockSnapshot.block("minecraft:stone"),
            List.of(10_000L + sequence)
        );
    }

    private static final class GeneratedOperationSource implements OperationItemSource {
        private final int itemCount;
        private int cursor;

        private GeneratedOperationSource(int itemCount) {
            this.itemCount = itemCount;
        }

        @Override
        public List<OperationItem> readBatch(int maximumItems) {
            List<OperationItem> batch = new ArrayList<>(Math.min(maximumItems, itemCount - cursor));
            while (cursor < itemCount && batch.size() < maximumItems) {
                batch.add(generatedItem(cursor++));
            }
            return List.copyOf(batch);
        }

        @Override
        public void close() {
        }
    }

    private static ChangeRecord change(
        long time,
        int x,
        int y,
        int z,
        String before,
        String after
    ) {
        return new ChangeRecord(
            0L,
            time,
            new BlockPosition(WORLD_ID, x, y, z),
            ACTOR,
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            ""
        );
    }

    private static ChangeRecord batchedChange(
        long time,
        int x,
        int y,
        int z,
        String before,
        String after,
        UUID batchId
    ) {
        return new ChangeRecord(
            0L,
            time,
            new BlockPosition(WORLD_ID, x, y, z),
            ACTOR,
            ChangeCause.WORLD_EDIT,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            batchId,
            ""
        );
    }

    private static long scalar(Statement statement, String sql) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private static String textScalar(Statement statement, String sql) throws Exception {
        try (ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }

    private static void createLegacyDatabase(Path file) throws Exception {
        Class.forName("org.sqlite.JDBC");
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + file.toAbsolutePath());
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("""
                CREATE TABLE changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    occurred_at INTEGER NOT NULL,
                    world_uuid TEXT NOT NULL,
                    x INTEGER NOT NULL,
                    y INTEGER NOT NULL,
                    z INTEGER NOT NULL,
                    actor_uuid TEXT,
                    actor_name TEXT NOT NULL,
                    actor_kind TEXT NOT NULL,
                    cause TEXT NOT NULL,
                    before_data TEXT NOT NULL,
                    before_payload_type TEXT,
                    before_payload BLOB,
                    after_data TEXT NOT NULL,
                    after_payload_type TEXT,
                    after_payload BLOB,
                    operation_uuid TEXT,
                    metadata TEXT
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE operations (
                    operation_uuid TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL,
                    completed_at INTEGER,
                    actor_uuid TEXT NOT NULL,
                    actor_name TEXT NOT NULL,
                    actor_kind TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    summary TEXT NOT NULL,
                    inverse_of TEXT,
                    item_count INTEGER NOT NULL,
                    applied_count INTEGER NOT NULL,
                    skipped_count INTEGER NOT NULL,
                    failure TEXT
                )
                """);
            statement.executeUpdate("""
                CREATE TABLE operation_items (
                    operation_uuid TEXT NOT NULL,
                    sequence INTEGER NOT NULL,
                    world_uuid TEXT NOT NULL,
                    x INTEGER NOT NULL,
                    y INTEGER NOT NULL,
                    z INTEGER NOT NULL,
                    before_data TEXT NOT NULL,
                    before_payload_type TEXT,
                    before_payload BLOB,
                    after_data TEXT NOT NULL,
                    after_payload_type TEXT,
                    after_payload BLOB,
                    source_ids TEXT,
                    applied INTEGER NOT NULL,
                    PRIMARY KEY (operation_uuid, sequence)
                )
                """);
            statement.executeUpdate("""
                INSERT INTO changes(
                    id, occurred_at, world_uuid, x, y, z, actor_uuid, actor_name, actor_kind,
                    cause, before_data, before_payload_type, before_payload,
                    after_data, after_payload_type, after_payload, operation_uuid, metadata
                ) VALUES (
                    7, 1234, '%s', 4, 70, -8, '%s', 'Builder', 'PLAYER',
                    'PLAYER_PLACE', 'minecraft:air', '', X'',
                    'minecraft:oak_planks', '', X'', NULL, 'legacy'
                )
                """.formatted(WORLD_ID, ACTOR_ID));
            statement.executeUpdate("PRAGMA user_version=1");
        }
    }
}
