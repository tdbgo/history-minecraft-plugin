package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncHistoryStoreTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void flushesQueuedChangesBeforeQueriesAndClose() throws Exception {
        UUID worldId = UUID.randomUUID();
        UUID actorId = UUID.randomUUID();
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("async.db"),
            1_000,
            32,
            20,
            1_000
        );
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger());
        try {
            boolean accepted = store.append(new ChangeRecord(
                0L,
                100L,
                new BlockPosition(worldId, 1, 64, 2),
                ActorRef.player(actorId, "Builder"),
                ChangeCause.PLAYER_PLACE,
                BlockSnapshot.air(),
                BlockSnapshot.block("minecraft:stone"),
                null,
                ""
            ));
            assertTrue(accepted);

            List<ChangeRecord> changes = store.query(
                HistoryQuery.at(worldId, 1, 64, 2, 0L, 10)
            ).get(5, TimeUnit.SECONDS);

            assertEquals(1, changes.size());
            assertEquals(1L, store.status().persisted());
            assertTrue(store.status().healthy());
        } finally {
            store.closeAsync().get(Duration.ofSeconds(5).toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void stopsAcceptingWhenBoundedQueueOverflowsBeforeStartupCompletes() throws Exception {
        HistoryConfig.Postgres unavailable = new HistoryConfig.Postgres(
            "127.0.0.1",
            1,
            "history",
            "history",
            "history",
            "test",
            "disable",
            100,
            1
        );
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.POSTGRESQL,
            temporaryDirectory.resolve("unused.db"),
            unavailable,
            1,
            2,
            5_000,
            1_000,
            0,
            10_000,
            60
        );
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger());
        UUID worldId = UUID.randomUUID();
        ChangeRecord change = new ChangeRecord(
            0L,
            100L,
            new BlockPosition(worldId, 1, 64, 2),
            ActorRef.player(UUID.randomUUID(), "Builder"),
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.air(),
            BlockSnapshot.block("minecraft:stone"),
            null,
            ""
        );

        assertTrue(store.append(change));
        assertFalse(store.append(change));
        assertFalse(store.status().accepting());
        assertFalse(store.status().healthy());
        assertEquals(1L, store.status().rejected());
        store.closeAsync().get(5, TimeUnit.SECONDS);
    }

    @Test
    void compactsOneWorldEditBatchBeforePersistenceWithoutChangingItsRollbackEndpoints() throws Exception {
        UUID worldId = UUID.randomUUID();
        UUID actorId = UUID.randomUUID();
        UUID batchId = UUID.randomUUID();
        BlockPosition position = new BlockPosition(worldId, 1, 64, 2);
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("compact.db"),
            1_000,
            32,
            5_000,
            1_000
        );
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger());
        try {
            assertTrue(store.append(new ChangeRecord(
                0L, 100L, position, ActorRef.player(actorId, "Builder"), ChangeCause.WORLD_EDIT,
                BlockSnapshot.block("minecraft:stone"), BlockSnapshot.block("minecraft:dirt"),
                null, batchId, ""
            )));
            assertTrue(store.append(new ChangeRecord(
                0L, 101L, position, ActorRef.player(actorId, "Builder"), ChangeCause.WORLD_EDIT,
                BlockSnapshot.block("minecraft:dirt"), BlockSnapshot.block("minecraft:gold_block"),
                null, batchId, ""
            )));

            List<ChangeRecord> changes = store.query(
                HistoryQuery.at(worldId, 1, 64, 2, 0L, 10)
            ).get(5, TimeUnit.SECONDS);

            assertEquals(1, changes.size());
            assertEquals("minecraft:stone", changes.getFirst().before().blockData());
            assertEquals("minecraft:gold_block", changes.getFirst().after().blockData());
            assertEquals(2L, store.status().accepted());
            assertEquals(1L, store.status().persisted());
            assertEquals(1L, store.status().compacted());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }
}
