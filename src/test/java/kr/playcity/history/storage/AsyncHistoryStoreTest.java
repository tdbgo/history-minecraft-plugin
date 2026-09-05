package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;
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
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.StoredOperation;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
            1,
            2,
            5_000,
            30_000,
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
        assertTrue(store.status().accepting());
        assertTrue(store.status().degraded());
        assertFalse(store.status().captureComplete());
        assertFalse(store.status().operational());
        assertEquals(1L, store.status().rejected());
        store.closeAsync().get(5, TimeUnit.SECONDS);
    }

    @Test
    void startupFailurePreservesEveryPreviouslyAcceptedChangeInTheDurableJournal() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("startup-failure.db"),
            8,
            4,
            20,
            1_000
        );
        TestRepository repository = new TestRepository(false, 0, true, true);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            assertTrue(repository.openStarted.await(2, TimeUnit.SECONDS));
            assertTrue(store.append(change(0)));
            assertTrue(store.tryAppendWorldEditBatch(List.of(change(1), change(2))));

            repository.releaseOpen.countDown();
            await(
                () -> !store.status().healthy() && store.status().databaseQueued() == 3,
                Duration.ofSeconds(2)
            );

            StoreStatus status = store.status();
            assertFalse(status.ready());
            assertTrue(status.accepting());
            assertFalse(status.healthy());
            assertFalse(status.degraded());
            assertTrue(status.captureComplete());
            assertEquals(3, status.databaseQueued());
            assertEquals(3L, status.accepted());
            assertEquals(0L, status.persisted());
            assertEquals(0L, status.captureGapChanges());
            assertEquals(0L, status.worldEditCaptureGapChanges());

            assertTrue(store.append(change(3)));
            await(() -> store.status().databaseQueued() == 4, Duration.ofSeconds(2));
            assertEquals(4L, store.status().accepted());
            assertEquals(0L, store.status().rejected());

            repository.allowOpen();
            await(
                () -> store.status().ready()
                    && store.status().databaseQueued() == 0
                    && store.status().persisted() == 4,
                Duration.ofSeconds(3)
            );
            assertTrue(store.status().healthy());
            assertTrue(store.status().captureComplete());
        } finally {
            repository.releaseOpen.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void preservesIntermediateWorldEditStatesForStableReplayIdentities() throws Exception {
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

            assertEquals(2, changes.size());
            assertEquals("minecraft:dirt", changes.getFirst().before().blockData());
            assertEquals("minecraft:gold_block", changes.getFirst().after().blockData());
            assertEquals("minecraft:stone", changes.getLast().before().blockData());
            assertEquals(2L, store.status().accepted());
            assertEquals(2L, store.status().persisted());
            assertEquals(0L, store.status().compacted());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void preservesDirectAndFaweAdmissionOrderAtTheSameTimestamp() throws Exception {
        TestRepository repository = new TestRepository(false, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(new HistoryConfig.Storage(
            temporaryDirectory.resolve("mixed-order.db"), 1_000, 512, 30_000, 1_000
        ), Logger.getAnonymousLogger(), repository);
        ChangeRecord sample = change(0);
        List<ChangeRecord> expected = new ArrayList<>();
        UUID edit = UUID.randomUUID();
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            for (int index = 0; index < 100; index++) {
                ChangeRecord next = new ChangeRecord(0, 100, sample.position(), sample.actor(),
                    index % 2 == 0 ? ChangeCause.PLAYER_PLACE : ChangeCause.WORLD_EDIT,
                    BlockSnapshot.block(index % 2 == 0 ? "minecraft:stone" : "minecraft:dirt"),
                    BlockSnapshot.block(index % 2 == 0 ? "minecraft:dirt" : "minecraft:stone"),
                    null, index % 2 == 0 ? null : edit, "");
                expected.add(next);
                assertTrue(index % 2 == 0 ? store.append(next) : store.tryAppendWorldEdit(next));
            }
            store.query(HistoryQuery.at(sample.position().worldId(), 0, 64, 2, 0, 100))
                .get(3, TimeUnit.SECONDS);
            assertEquals(expected, repository.writtenChanges);
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void resumeCannotReopenCaptureAfterShutdownBegins() throws Exception {
        TestRepository repository = new TestRepository(false, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(new HistoryConfig.Storage(
            temporaryDirectory.resolve("resume-close.db"), 32, 8, 20, 1_000
        ), Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            store.reportCaptureGap(1, "direct", "test gap");
            repository.blockVerification.set(true);
            CompletableFuture<CaptureRecoveryResult> resumed = store.resumeCapture();
            assertTrue(repository.verificationStarted.await(2, TimeUnit.SECONDS));
            CompletableFuture<Void> closed = store.closeAsync();
            repository.releaseVerification.countDown();
            assertFalse(resumed.get(3, TimeUnit.SECONDS).resumed());
            closed.get(3, TimeUnit.SECONDS);
            assertFalse(store.status().accepting());
        } finally {
            repository.releaseVerification.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void startupReplayDoesNotDuplicateABatchCommittedBeforeItsJournalCheckpoint() throws Exception {
        Path database = temporaryDirectory.resolve("commit-before-checkpoint.db");
        HistoryConfig.Storage config = new HistoryConfig.Storage(database, 32, 8, 20, 1_000);
        ChangeRecord committed = change(40);
        SqliteHistoryRepository repository = new SqliteHistoryRepository(database, 1_000);
        repository.open();
        repository.insertBatch(List.of(committed));
        repository.close();
        try (CaptureJournal journal = new CaptureJournal(
            database.resolveSibling("capture-journal.wal")
        )) {
            journal.open();
            journal.append(List.of(committed));
        }

        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger());
        try {
            await(
                () -> store.status().ready() && store.status().databaseQueued() == 0,
                Duration.ofSeconds(3)
            );
            List<ChangeRecord> stored = store.query(HistoryQuery.at(
                committed.position().worldId(),
                committed.position().x(),
                committed.position().y(),
                committed.position().z(),
                0L,
                10
            )).get(3, TimeUnit.SECONDS);
            assertEquals(1, stored.size());
            assertEquals(0L, store.status().captureGapEvents());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void startupReportsAnIncompleteCrashTailWhileReplayingEveryCompleteFrame() throws Exception {
        Path database = temporaryDirectory.resolve("partial-crash-tail.db");
        HistoryConfig.Storage config = new HistoryConfig.Storage(database, 32, 8, 20, 1_000);
        ChangeRecord complete = change(41);
        Path journalFile = database.resolveSibling("capture-journal.wal");
        try (CaptureJournal journal = new CaptureJournal(journalFile)) {
            journal.open();
            journal.append(List.of(complete));
        }
        java.nio.file.Files.write(
            journalFile,
            new byte[] {0, 0, 0, 64},
            java.nio.file.StandardOpenOption.APPEND
        );

        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger());
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(3));
            StoreStatus status = store.status();
            assertEquals(1L, status.captureGapEvents());
            assertEquals(1L, status.unknownCaptureGapEvents());
            assertFalse(status.captureComplete());
            assertTrue(status.degraded());
            List<ChangeRecord> stored = store.query(HistoryQuery.at(
                complete.position().worldId(),
                complete.position().x(),
                complete.position().y(),
                complete.position().z(),
                0L,
                10
            )).get(3, TimeUnit.SECONDS);
            assertEquals(1, stored.size());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void backpressuresWorldEditBurstsWithoutRejectingRecords() throws Exception {
        UUID worldId = UUID.randomUUID();
        UUID actorId = UUID.randomUUID();
        UUID batchId = UUID.randomUUID();
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("worldedit-backpressure.db"),
            8,
            4,
            20,
            1_000
        );
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger());
        try {
            for (int index = 0; index < 2_048; index++) {
                assertTrue(store.appendWorldEdit(new ChangeRecord(
                    0L,
                    100L + index,
                    new BlockPosition(worldId, index, 64, 2),
                    ActorRef.player(actorId, "Builder"),
                    ChangeCause.WORLD_EDIT,
                    BlockSnapshot.air(),
                    BlockSnapshot.block("minecraft:stone"),
                    null,
                    batchId,
                    ""
                )));
            }

            store.query(HistoryQuery.at(worldId, 0, 64, 2, 0L, 10))
                .get(10, TimeUnit.SECONDS);

            assertEquals(2_048L, store.status().accepted());
            assertEquals(2_048L, store.status().persisted());
            assertEquals(0L, store.status().rejected());
            assertTrue(store.status().accepting());
            assertTrue(store.status().healthy());
        } finally {
            store.closeAsync().get(Duration.ofSeconds(10).toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @Test
    void databaseStallDrainsCaptureIntoTheJournalWithoutOverflowOrRecovery() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("overflow-recovery.db"),
            64,
            1,
            20,
            1_000
        );
        TestRepository repository = new TestRepository(true, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));

            assertTrue(store.append(change(0)));
            assertTrue(repository.firstInsertStarted.await(2, TimeUnit.SECONDS));
            for (int index = 1; index <= 32; index++) {
                assertTrue(store.append(change(index)));
            }
            assertTrue(store.status().accepting());
            assertFalse(store.status().degraded());
            assertTrue(store.status().captureComplete());
            assertEquals(0L, store.status().rejected());

            repository.releaseFirstInsert.countDown();
            await(
                () -> store.status().queued() == 0 && store.status().persisted() == 33,
                Duration.ofSeconds(5)
            );

            StoreStatus drained = store.status();
            assertTrue(drained.healthy());
            assertTrue(drained.accepting());
            assertFalse(drained.degraded());
            assertTrue(drained.captureComplete());
            assertTrue(drained.operational());
            assertEquals(0L, drained.rejected());
        } finally {
            repository.releaseFirstInsert.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void recoversTransientRepositoryFailureWithoutCreatingCaptureGap() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("transient-storage.db"),
            8,
            1,
            20,
            1_000
        );
        TestRepository repository = new TestRepository(false, 1);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertTrue(store.append(change(0)));
            await(() -> store.status().persisted() == 1, Duration.ofSeconds(5));

            StoreStatus recovered = store.status();
            assertTrue(recovered.healthy());
            assertTrue(recovered.accepting());
            assertTrue(recovered.captureComplete());
            assertTrue(recovered.operational());
            assertEquals(0L, recovered.rejected());
            assertEquals("", recovered.lastError());
            assertEquals(2, repository.insertAttempts.get());
            // A successful retry must also advance the read/rollback barrier.
            store.query(HistoryQuery.at(change(0).position().worldId(), 0, 64, 2, 0L, 10))
                .get(2, TimeUnit.SECONDS);
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void retriesTransientFailureDuringStartupReplay() throws Exception {
        Path database = temporaryDirectory.resolve("replay-startup-failure.db");
        try (CaptureJournal journal = new CaptureJournal(database.resolveSibling("capture-journal.wal"))) {
            journal.open();
            journal.append(List.of(change(0), change(1)));
        }
        TestRepository repository = new TestRepository(false, 1);
        AsyncHistoryStore store = new AsyncHistoryStore(
            new HistoryConfig.Storage(database, 32, 8, 20, 1_000), Logger.getAnonymousLogger(), repository
        );
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(3));
            assertEquals(2L, store.status().persisted());
            assertEquals(0, store.status().databaseQueued());
            assertTrue(store.append(change(2)));
            store.query(HistoryQuery.at(change(2).position().worldId(), 2, 64, 2, 0L, 10))
                .get(2, TimeUnit.SECONDS);
            assertEquals(3L, store.status().persisted());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void replayWithDifferentBatchBoundariesPreservesIntermediateStates() throws Exception {
        Path database = temporaryDirectory.resolve("replay-regrouped.db");
        ChangeRecord first = change(10);
        UUID edit = UUID.randomUUID();
        first = new ChangeRecord(0, 100, first.position(), first.actor(), ChangeCause.WORLD_EDIT,
            BlockSnapshot.air(), BlockSnapshot.block("minecraft:stone"), null, edit, "");
        ChangeRecord second = new ChangeRecord(0, 101, first.position(), first.actor(), ChangeCause.WORLD_EDIT,
            first.after(), BlockSnapshot.block("minecraft:dirt"), null, edit, "");
        SqliteHistoryRepository repository = new SqliteHistoryRepository(database, 1_000);
        repository.open();
        repository.insertBatch(List.of(first));
        repository.close();
        try (CaptureJournal journal = new CaptureJournal(database.resolveSibling("capture-journal.wal"))) {
            journal.open();
            journal.append(List.of(first, second));
        }
        AsyncHistoryStore store = new AsyncHistoryStore(
            new HistoryConfig.Storage(database, 32, 8, 20, 1_000), Logger.getAnonymousLogger()
        );
        try {
            List<ChangeRecord> result = store.query(HistoryQuery.at(first.position().worldId(), 10, 64, 2, 0L, 10))
                .get(3, TimeUnit.SECONDS);
            assertEquals(2, result.size());
            assertEquals("minecraft:stone", result.getFirst().before().blockData());
            assertEquals("minecraft:dirt", result.getFirst().after().blockData());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void databaseStallDoesNotConsumeWorldEditAdmissionCapacity() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("worldedit-timeout.db"),
            HistoryConfig.Postgres.defaults(),
            1,
            1,
            1,
            20,
            50,
            1_000,
            0,
            10_000,
            60
        );
        TestRepository repository = new TestRepository(true, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertTrue(store.appendWorldEdit(change(0)));
            assertTrue(repository.firstInsertStarted.await(2, TimeUnit.SECONDS));
            assertTrue(store.appendWorldEdit(change(1)));
            assertTrue(store.appendWorldEdit(change(2)));

            StoreStatus blocked = store.status();
            assertEquals(0L, blocked.blockedWorldEdits());
            assertEquals(0L, blocked.rejected());
            assertTrue(blocked.captureComplete());
            assertFalse(blocked.degraded());
            assertTrue(blocked.accepting());
        } finally {
            repository.releaseFirstInsert.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void worldEditFallbackJournalsImmediatelyWithoutBlockingOnTheDatabase() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("worldedit-nonblocking.db"),
            HistoryConfig.Postgres.defaults(),
            1,
            1,
            1,
            20,
            300_000,
            1_000,
            0,
            10_000,
            60
        );
        TestRepository repository = new TestRepository(true, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertTrue(store.tryAppendWorldEdit(change(0)));
            assertTrue(repository.firstInsertStarted.await(2, TimeUnit.SECONDS));
            assertTrue(store.tryAppendWorldEdit(change(1)));

            long started = System.nanoTime();
            assertTrue(store.tryAppendWorldEdit(change(2)));
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);

            assertTrue(elapsedMillis < 2_000L);
            assertEquals(0L, store.status().blockedWorldEdits());
            assertEquals(0L, store.status().rejected());
            assertTrue(store.status().captureComplete());
            assertFalse(store.status().degraded());
        } finally {
            repository.releaseFirstInsert.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerCaptureDoesNotWaitForAStalledDatabaseAfterJournalAdmission() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("worldedit-worker-backpressure.db"),
            HistoryConfig.Postgres.defaults(),
            1,
            1,
            1,
            20,
            2_000,
            1_000,
            0,
            10_000,
            60
        );
        TestRepository repository = new TestRepository(true, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertTrue(store.tryAppendWorldEdit(change(0)));
            assertTrue(repository.firstInsertStarted.await(2, TimeUnit.SECONDS));
            assertTrue(store.tryAppendWorldEdit(change(1)));

            CompletableFuture<Boolean> admitted = CompletableFuture.supplyAsync(() ->
                store.appendWorldEditBatch(List.of(change(2)))
            );
            assertTrue(admitted.get(2, TimeUnit.SECONDS));

            repository.releaseFirstInsert.countDown();
            await(() -> store.status().persisted() == 3L, Duration.ofSeconds(2));
            assertEquals(0L, store.status().captureGapEvents());
        } finally {
            repository.releaseFirstInsert.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void workerBackpressureStreamsABatchLargerThanInternalQueueCapacity() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("worldedit-large-worker-batch.db"),
            HistoryConfig.Postgres.defaults(),
            2,
            2,
            1,
            20,
            2_000,
            1_000,
            0,
            10_000,
            60
        );
        TestRepository repository = new TestRepository(false, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            List<ChangeRecord> changes = new ArrayList<>();
            for (int index = 0; index < 17; index++) {
                changes.add(change(index));
            }
            assertTrue(store.appendWorldEditBatch(changes));
            await(() -> store.status().persisted() == 17L, Duration.ofSeconds(2));
            assertEquals(0L, store.status().captureGapEvents());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void oversizedWorkerBatchStreamsCompletelyWhileTheDatabaseIsStalled() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("worldedit-large-worker-gap.db"),
            HistoryConfig.Postgres.defaults(),
            2,
            2,
            1,
            20,
            100,
            1_000,
            0,
            10_000,
            60
        );
        TestRepository repository = new TestRepository(true, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            List<ChangeRecord> changes = new ArrayList<>();
            for (int index = 0; index < 7; index++) {
                changes.add(change(index));
            }

            CompletableFuture<Boolean> recorded = CompletableFuture.supplyAsync(() ->
                store.appendWorldEditBatch(changes)
            );
            assertTrue(repository.firstInsertStarted.await(2, TimeUnit.SECONDS));
            assertTrue(recorded.get(2, TimeUnit.SECONDS));

            StoreStatus status = store.status();
            assertEquals(7L, status.accepted());
            assertEquals(0L, status.captureGapChanges());
            assertEquals(0L, status.captureGapEvents());
        } finally {
            repository.releaseFirstInsert.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void admitsAppliedFaweChunkAsOneBoundedBatchWithoutAReservationLifecycle() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("fawe-batch-reservation.db"),
            HistoryConfig.Postgres.defaults(),
            1,
            2,
            1,
            20,
            1_000,
            1_000,
            0,
            10_000,
            60
        );
        TestRepository repository = new TestRepository(false, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertFalse(store.tryAppendWorldEditBatch(List.of(change(10), change(11), change(12))));
            assertEquals(0L, store.status().accepted());
            assertEquals(0, store.status().pendingReservations());
            assertEquals(1L, store.status().captureGapEvents());

            assertTrue(store.tryAppendWorldEditBatch(List.of(change(0), change(1))));

            await(() -> store.status().persisted() == 2, Duration.ofSeconds(5));
            assertEquals(2L, store.status().accepted());
            assertEquals(2L, store.status().persisted());
            assertEquals(0, store.status().pendingReservations());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void externalCaptureLifecycleIsIdempotentAndDiagnosesAbandonment() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("external-capture.db"), 8, 2, 20, 1_000
        );
        TestRepository repository = new TestRepository(false, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            UUID completed = UUID.randomUUID();
            store.beginExternalCapture(completed, 42, "fawe");
            assertEquals(1, store.status().pendingReservations());
            assertEquals(42L, store.status().pendingReservationChanges());
            assertEquals(completed.toString(), store.status().oldestReservationId());
            store.completeExternalCapture(completed);
            store.completeExternalCapture(completed);
            assertEquals(0, store.status().pendingReservations());
            assertEquals(0L, store.status().captureGapEvents());

            UUID abandoned = UUID.randomUUID();
            store.beginExternalCapture(abandoned, 17, "fawe");
            store.abandonExternalCapture(abandoned, "test cancellation");
            store.abandonExternalCapture(abandoned, "duplicate cancellation");
            assertEquals(0, store.status().pendingReservations());
            assertEquals(1L, store.status().captureGapEvents());
            assertEquals(17L, store.status().worldEditCaptureGapChanges());
            assertTrue(store.status().degraded());
            assertTrue(store.resumeCapture().get(5, TimeUnit.SECONDS).resumed());
            assertFalse(store.status().degraded());
            assertEquals(1L, store.status().captureGapEvents());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void retriesDurableOperationCheckpointUntilStorageRecovers() throws Exception {
        TestRepository repository = new TestRepository(false, 0);
        repository.operationFailuresRemaining.set(2);
        HistoryConfig.Storage operationConfig = new HistoryConfig.Storage(
            kr.playcity.history.config.StorageBackend.SQLITE,
            temporaryDirectory.resolve("operation-retry.db"),
            HistoryConfig.Postgres.defaults(),
            8,
            8,
            4,
            20,
            1_000,
            1_000,
            0,
            10_000,
            60
        );
        AsyncHistoryStore store = new AsyncHistoryStore(
            operationConfig,
            Logger.getAnonymousLogger(),
            repository
        );
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            OperationItem item = new OperationItem(
                0,
                change(0).position(),
                BlockSnapshot.block("minecraft:stone"),
                BlockSnapshot.block("minecraft:dirt"),
                List.of(1L)
            );
            store.checkpointOperation(new OperationCheckpoint(
                UUID.randomUUID(),
                1_000L,
                List.of(new AppliedOperationItem(item, item.before(), item.after()))
            )).get(5, TimeUnit.SECONDS);

            assertEquals(3, repository.operationAttempts.get());
            assertTrue(store.status().healthy());
        } finally {
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void concurrentCloseCallsFlushAndCloseTheRepositoryExactlyOnce() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("concurrent-close.db"),
            32,
            8,
            20,
            1_000
        );
        TestRepository repository = new TestRepository(false, 0);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        await(() -> store.status().ready(), Duration.ofSeconds(2));
        assertTrue(store.append(change(0)));

        List<CompletableFuture<Void>> closes = Collections.synchronizedList(new ArrayList<>());
        CountDownLatch start = new CountDownLatch(1);
        List<Thread> callers = new ArrayList<>();
        for (int index = 0; index < 32; index++) {
            Thread caller = Thread.ofPlatform().unstarted(() -> {
                try {
                    start.await();
                    closes.add(store.closeAsync());
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(interrupted);
                }
            });
            callers.add(caller);
            caller.start();
        }
        start.countDown();
        for (Thread caller : callers) {
            caller.join();
        }
        CompletableFuture.allOf(closes.toArray(CompletableFuture[]::new)).get(5, TimeUnit.SECONDS);

        assertEquals(32, closes.size());
        assertTrue(closes.stream().allMatch(future -> future == closes.getFirst()));
        assertEquals(1L, repository.persisted.get());
        assertEquals(1, repository.closeAttempts.get());
        assertEquals(0, store.status().databaseQueued());
    }

    @Test
    void finalDatabaseFailureClosesWithEveryAcceptedChangeStillJournaled() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("failed-close.db"),
            8,
            1,
            20,
            1_000
        );
        TestRepository repository = new TestRepository(true, 100);
        AsyncHistoryStore store = new AsyncHistoryStore(config, Logger.getAnonymousLogger(), repository);
        await(() -> store.status().ready(), Duration.ofSeconds(2));
        assertTrue(store.append(change(0)));
        assertTrue(repository.firstInsertStarted.await(2, TimeUnit.SECONDS));
        assertTrue(store.append(change(1)));

        CompletableFuture<Void> close = store.closeAsync();
        repository.releaseFirstInsert.countDown();
        close.get(5, TimeUnit.SECONDS);

        StoreStatus status = store.status();
        assertEquals(1, repository.closeAttempts.get());
        assertEquals(2, status.databaseQueued());
        assertEquals(2L, status.accepted());
        assertEquals(0L, status.persisted());
        assertEquals(0L, status.captureGapChanges());
        assertTrue(status.captureComplete());
        assertFalse(status.healthy());
    }

    @Test
    void longRunningReadDoesNotBlockConcurrentHistoryPersistence() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("read-write-isolation.db"),
            32,
            8,
            20,
            1_000
        );
        TestRepository writer = new TestRepository(false, 0);
        TestRepository reader = new TestRepository(false, 0, false, false, true);
        AsyncHistoryStore store = new AsyncHistoryStore(
            config,
            Logger.getAnonymousLogger(),
            writer,
            reader
        );
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            CompletableFuture<List<ChangeRecord>> query = store.query(
                HistoryQuery.at(change(0).position().worldId(), 0, 64, 2, 0L, 10)
            );
            assertTrue(reader.queryStarted.await(2, TimeUnit.SECONDS));

            assertTrue(store.append(change(0)));
            await(
                () -> writer.persisted.get() == 1L && store.status().persisted() == 1L,
                Duration.ofSeconds(2)
            );
            assertFalse(query.isDone());

            reader.releaseQuery.countDown();
            assertTrue(query.get(2, TimeUnit.SECONDS).isEmpty());
        } finally {
            reader.releaseQuery.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
        assertEquals(1, writer.closeAttempts.get());
        assertEquals(1, reader.closeAttempts.get());
    }

    @Test
    void readBarrierDoesNotWaitForTrafficAcceptedAfterTheQueryBegan() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("finite-read-barrier.db"),
            64,
            1,
            20,
            1_000
        );
        TestRepository writer = new TestRepository(true, 0);
        TestRepository reader = new TestRepository(false, 0, false, false, true);
        AsyncHistoryStore store = new AsyncHistoryStore(
            config,
            Logger.getAnonymousLogger(),
            writer,
            reader
        );
        try {
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertTrue(store.append(change(0)));
            assertTrue(writer.firstInsertStarted.await(2, TimeUnit.SECONDS));

            CompletableFuture<List<ChangeRecord>> query = store.query(
                HistoryQuery.at(change(0).position().worldId(), 0, 64, 2, 0L, 10)
            );
            for (int index = 1; index <= 16; index++) {
                assertTrue(store.append(change(index)));
            }
            writer.releaseFirstInsert.countDown();

            assertTrue(reader.queryStarted.await(2, TimeUnit.SECONDS));
            assertTrue(store.status().databaseQueued() > 0);
            reader.releaseQuery.countDown();
            assertTrue(query.get(2, TimeUnit.SECONDS).isEmpty());
        } finally {
            writer.releaseFirstInsert.countDown();
            reader.releaseQuery.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
        assertEquals(17L, writer.persisted.get());
    }

    @Test
    void writerPersistsStartupChangesWhileTheReadConnectionIsStillOpening() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("slow-reader-startup.db"),
            32,
            8,
            20,
            1_000
        );
        TestRepository writer = new TestRepository(false, 0);
        TestRepository reader = new TestRepository(false, 0, true, false);
        AsyncHistoryStore store = new AsyncHistoryStore(
            config,
            Logger.getAnonymousLogger(),
            writer,
            reader
        );
        try {
            assertTrue(reader.openStarted.await(2, TimeUnit.SECONDS));
            assertFalse(store.status().ready());
            assertTrue(store.append(change(0)));
            await(
                () -> writer.persisted.get() == 1L && store.status().persisted() == 1L,
                Duration.ofSeconds(2)
            );

            reader.releaseOpen.countDown();
            await(() -> store.status().ready(), Duration.ofSeconds(2));
            assertEquals(1L, store.status().persisted());
            assertEquals(0L, store.status().captureGapEvents());
        } finally {
            reader.releaseOpen.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void readStartupFailureKeepsWriterRunningAndRecoversAutomatically() throws Exception {
        HistoryConfig.Storage config = new HistoryConfig.Storage(
            temporaryDirectory.resolve("reader-startup-failure.db"),
            32,
            1,
            20,
            1_000
        );
        TestRepository writer = new TestRepository(false, 0);
        TestRepository reader = new TestRepository(false, 0, true, true);
        AsyncHistoryStore store = new AsyncHistoryStore(
            config,
            Logger.getAnonymousLogger(),
            writer,
            reader
        );
        try {
            assertTrue(reader.openStarted.await(2, TimeUnit.SECONDS));
            assertTrue(store.append(change(0)));
            await(
                () -> writer.persisted.get() == 1L && store.status().persisted() == 1L,
                Duration.ofSeconds(2)
            );

            reader.releaseOpen.countDown();
            await(() -> !store.status().healthy(), Duration.ofSeconds(2));
            StoreStatus failed = store.status();
            assertFalse(failed.ready());
            assertTrue(failed.accepting());
            assertEquals(1L, failed.accepted());
            assertEquals(1L, failed.persisted());
            assertTrue(failed.captureComplete());

            assertTrue(store.append(change(1)));
            await(
                () -> writer.persisted.get() == 2L && store.status().persisted() == 2L,
                Duration.ofSeconds(2)
            );
            assertFalse(store.status().healthy());
            reader.allowOpen();
            await(() -> store.status().ready() && store.status().healthy(), Duration.ofSeconds(3));
            assertEquals(0L, store.status().captureGapEvents());
        } finally {
            reader.releaseOpen.countDown();
            store.closeAsync().get(5, TimeUnit.SECONDS);
        }
    }

    private static ChangeRecord change(int index) {
        return new ChangeRecord(
            0L,
            100L + index,
            new BlockPosition(UUID.fromString("11111111-1111-1111-1111-111111111111"), index, 64, 2),
            ActorRef.player(UUID.fromString("22222222-2222-2222-2222-222222222222"), "Builder"),
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.air(),
            BlockSnapshot.block("minecraft:stone"),
            null,
            ""
        );
    }

    private static void await(BooleanSupplier condition, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() >= deadline) {
                fail("condition did not become true within " + timeout);
            }
            Thread.sleep(5L);
        }
    }

    private static final class TestRepository implements HistoryRepository {
        private final boolean blockFirstInsert;
        private final AtomicInteger failuresRemaining;
        private final CountDownLatch firstInsertStarted = new CountDownLatch(1);
        private final CountDownLatch releaseFirstInsert = new CountDownLatch(1);
        private final AtomicInteger insertAttempts = new AtomicInteger();
        private final AtomicLong persisted = new AtomicLong();
        private final AtomicInteger operationFailuresRemaining = new AtomicInteger();
        private final AtomicInteger operationAttempts = new AtomicInteger();
        private final AtomicInteger closeAttempts = new AtomicInteger();
        private final List<Integer> batchSizes = new ArrayList<>();
        private final List<ChangeRecord> writtenChanges = new ArrayList<>();
        private final boolean blockOpen;
        private final AtomicBoolean failOpen;
        private final boolean blockQuery;
        private final CountDownLatch openStarted = new CountDownLatch(1);
        private final CountDownLatch releaseOpen = new CountDownLatch(1);
        private final CountDownLatch queryStarted = new CountDownLatch(1);
        private final CountDownLatch releaseQuery = new CountDownLatch(1);
        private final AtomicBoolean blockVerification = new AtomicBoolean();
        private final CountDownLatch verificationStarted = new CountDownLatch(1);
        private final CountDownLatch releaseVerification = new CountDownLatch(1);

        private TestRepository(boolean blockFirstInsert, int failures) {
            this(blockFirstInsert, failures, false, false, false);
        }

        private TestRepository(boolean blockFirstInsert, int failures, boolean blockOpen, boolean failOpen) {
            this(blockFirstInsert, failures, blockOpen, failOpen, false);
        }

        private TestRepository(
            boolean blockFirstInsert,
            int failures,
            boolean blockOpen,
            boolean failOpen,
            boolean blockQuery
        ) {
            this.blockFirstInsert = blockFirstInsert;
            this.failuresRemaining = new AtomicInteger(failures);
            this.blockOpen = blockOpen;
            this.failOpen = new AtomicBoolean(failOpen);
            this.blockQuery = blockQuery;
        }

        @Override
        public void open() {
            openStarted.countDown();
            if (blockOpen) {
                try {
                    if (!releaseOpen.await(5, TimeUnit.SECONDS)) {
                        throw new StorageException("test repository open was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("test repository open was interrupted", interrupted);
                }
            }
            if (failOpen.get()) {
                throw new StorageException("test startup failure");
            }
        }

        private void allowOpen() {
            failOpen.set(false);
        }

        @Override
        public void insertBatch(List<ChangeRecord> changes) {
            int attempt = insertAttempts.incrementAndGet();
            firstInsertStarted.countDown();
            if (blockFirstInsert && attempt == 1) {
                try {
                    if (!releaseFirstInsert.await(5, TimeUnit.SECONDS)) {
                        throw new StorageException("test repository was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("test repository was interrupted", interrupted);
                }
            }
            if (failuresRemaining.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                throw new StorageException("transient test failure");
            }
            synchronized (batchSizes) {
                batchSizes.add(changes.size());
            }
            persisted.addAndGet(changes.size());
            writtenChanges.addAll(changes);
        }

        @Override
        public List<ChangeRecord> query(HistoryQuery query) {
            queryStarted.countDown();
            if (blockQuery) {
                try {
                    if (!releaseQuery.await(5, TimeUnit.SECONDS)) {
                        throw new StorageException("test query was not released");
                    }
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("test query was interrupted", interrupted);
                }
            }
            return List.of();
        }

        @Override
        public Map<BlockPosition, ChangeRecord> latestChanges(List<BlockPosition> positions) {
            return Map.of();
        }

        @Override
        public void prepareOperation(OperationDraft operation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void checkpointOperation(OperationCheckpoint checkpoint) {
            operationAttempts.incrementAndGet();
            if (operationFailuresRemaining.getAndUpdate(value -> Math.max(0, value - 1)) > 0) {
                throw new StorageException("transient operation failure");
            }
        }

        @Override
        public void completeOperation(OperationCompletion completion) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<StoredOperation> loadOperation(UUID operationId) {
            return Optional.empty();
        }

        @Override
        public Optional<StoredOperation> findLastOperation(UUID actorId) {
            return Optional.empty();
        }

        @Override
        public int interruptedOperationCount() {
            if (blockVerification.get()) {
                verificationStarted.countDown();
                try {
                    if (!releaseVerification.await(5, TimeUnit.SECONDS)) {
                        throw new StorageException("test verification was not released");
                    }
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                    throw new StorageException("test verification was interrupted", failure);
                }
            }
            return 0;
        }

        @Override
        public int purgeChangesBefore(long cutoffMillis, int limit) {
            return 0;
        }

        @Override
        public StorageProfile storageProfile() {
            return new StorageProfile("test", 0L, 0L, List.of());
        }

        @Override
        public String backendName() {
            return "test";
        }

        @Override
        public void close() {
            closeAttempts.incrementAndGet();
        }

        private int maximumBatchSize() {
            synchronized (batchSizes) {
                return batchSizes.stream().mapToInt(Integer::intValue).max().orElse(0);
            }
        }
    }
}
