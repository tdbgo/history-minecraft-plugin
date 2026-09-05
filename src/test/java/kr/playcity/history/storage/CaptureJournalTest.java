package kr.playcity.history.storage;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ArrayList;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CaptureJournalTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void durablyReplaysAnUnacknowledgedBatchAndCompactsAfterAcknowledgement() throws Exception {
        Path file = temporaryDirectory.resolve("capture.wal");
        ChangeRecord first = change(1);
        ChangeRecord second = change(2);
        try (CaptureJournal journal = new CaptureJournal(file)) {
            journal.open();
            journal.append(List.of(first, second));
            CaptureJournal.JournalBatch current = journal.readBatch(10);
            assertFalse(current.replayed());
            assertEquals(List.of(first, second), current.changes());
            assertEquals(2L, journal.pendingCount());
        }

        try (CaptureJournal recovered = new CaptureJournal(file)) {
            recovered.open();
            CaptureJournal.JournalBatch replay = recovered.readBatch(10);
            assertTrue(replay.replayed());
            assertEquals(List.of(first, second), replay.changes());
            recovered.acknowledge(replay);
            assertEquals(0L, recovered.pendingCount());
            assertEquals(0L, recovered.backlogBytes());
            assertEquals(8L, Files.size(file));
        }

        try (CaptureJournal empty = new CaptureJournal(file)) {
            empty.open();
            assertTrue(empty.readBatch(10).changes().isEmpty());
        }
    }

    @Test
    void discardsOnlyAnIncompleteCrashTailAndKeepsEveryCompleteFrameVisible() throws Exception {
        Path file = temporaryDirectory.resolve("partial-tail.wal");
        ChangeRecord complete = change(3);
        try (CaptureJournal journal = new CaptureJournal(file)) {
            journal.open();
            journal.append(List.of(complete));
        }
        Files.write(file, new byte[] {0, 0, 0, 32}, StandardOpenOption.APPEND);

        try (CaptureJournal recovered = new CaptureJournal(file)) {
            recovered.open();
            assertTrue(recovered.truncatedTail());
            CaptureJournal.JournalBatch replay = recovered.readBatch(10);
            assertEquals(List.of(complete), replay.changes());
        }
    }

    @Test
    void marksAnUncertainDatabaseCommitForIdempotentRetry() {
        Path file = temporaryDirectory.resolve("uncertain-commit.wal");
        try (CaptureJournal journal = new CaptureJournal(file)) {
            journal.open();
            journal.append(List.of(change(5), change(6), change(7)));
            CaptureJournal.JournalBatch firstAttempt = journal.readBatch(10);
            assertFalse(firstAttempt.replayed());

            journal.requireIdempotentRetry(firstAttempt);
            CaptureJournal.JournalBatch retry = journal.readBatch(10);
            assertTrue(retry.replayed());
            assertEquals(firstAttempt.changes(), retry.changes());
            assertFalse(retry.recovered());
            journal.acknowledge(retry);
            assertEquals(0L, journal.pendingCount());
        }
    }

    @Test
    void checkpointFailureCannotTruncateTheJournalOrForgetAnUnacknowledgedBatch() throws Exception {
        Path file = temporaryDirectory.resolve("checkpoint-failure.wal");
        Path temporaryCheckpoint = file.resolveSibling(file.getFileName() + ".checkpoint.tmp");
        try (CaptureJournal journal = new CaptureJournal(file)) {
            journal.open();
            journal.append(List.of(change(1), change(2)));
            journal.acknowledge(journal.readBatch(1));
            CaptureJournal.JournalBatch last = journal.readBatch(1);
            long length = Files.size(file);
            Files.createDirectory(temporaryCheckpoint);
            assertThrows(StorageException.class, () -> journal.acknowledge(last));
            assertEquals(length, Files.size(file));
            assertEquals(1L, journal.pendingCount());
            assertThrows(StorageException.class, () -> journal.append(List.of(change(3))));
            Files.delete(temporaryCheckpoint);
            journal.requireIdempotentRetry(last);
            journal.acknowledge(journal.readBatch(1));
            journal.append(List.of(change(3), change(4)));
        }
        try (CaptureJournal reopened = new CaptureJournal(file)) {
            reopened.open();
            assertEquals(2L, reopened.pendingCount());
            assertEquals(3, reopened.readBatch(10).changes().getFirst().position().x());
        }
    }

    @Test
    void staleAcknowledgementCannotConsumeNewRecordsAtReusedOffsets() {
        try (CaptureJournal journal = new CaptureJournal(temporaryDirectory.resolve("stale-ack.wal"))) {
            journal.open();
            journal.append(List.of(change(1)));
            CaptureJournal.JournalBatch first = journal.readBatch(1);
            journal.acknowledge(first);
            journal.append(List.of(change(2)));
            CaptureJournal.JournalBatch next = journal.readBatch(1);
            assertThrows(StorageException.class, () -> journal.acknowledge(first));
            assertThrows(StorageException.class, () -> journal.requireIdempotentRetry(first));
            assertEquals(1L, journal.pendingCount());
            journal.acknowledge(next);
        }
    }

    @Test
    void retriesDoNotAbsorbNewAppendsIntoAnUncertainBatch() {
        try (CaptureJournal journal = new CaptureJournal(temporaryDirectory.resolve("retry-boundary.wal"))) {
            journal.open();
            journal.append(List.of(change(1), change(2)));
            CaptureJournal.JournalBatch original = journal.readBatch(10);
            journal.append(List.of(change(3)));
            journal.requireIdempotentRetry(original);
            CaptureJournal.JournalBatch retry = journal.readBatch(10);
            assertEquals(original.changes(), retry.changes());
            journal.acknowledge(retry);
            assertEquals(List.of(change(3).position()), journal.readBatch(10).changes().stream()
                .map(ChangeRecord::position).toList());
        }
    }

    @Test
    void rejectsChecksumCorruptionInsteadOfSilentlySkippingHistory() throws Exception {
        Path file = temporaryDirectory.resolve("corrupt.wal");
        try (CaptureJournal journal = new CaptureJournal(file)) {
            journal.open();
            journal.append(List.of(change(4)));
        }
        try (RandomAccessFile corrupt = new RandomAccessFile(file.toFile(), "rw")) {
            long position = corrupt.length() - 1L;
            corrupt.seek(position);
            int value = corrupt.readUnsignedByte();
            corrupt.seek(position);
            corrupt.writeByte(value ^ 0xff);
        }

        CaptureJournal corrupted = new CaptureJournal(file);
        assertThrows(StorageException.class, corrupted::open);
    }

    @Test
    void boundsLargePayloadBatchesByBytesAsWellAsRecordCount() {
        try (CaptureJournal journal = new CaptureJournal(temporaryDirectory.resolve("payload-budget.wal"))) {
            journal.open();
            for (int index = 0; index < 20; index++) {
                ChangeRecord source = change(index);
                journal.append(List.of(new ChangeRecord(
                    source.id(), source.occurredAt(), source.position(), source.actor(), source.cause(),
                    source.before(), new BlockSnapshot("minecraft:chest", "inventory/v1", new byte[1_048_576]),
                    source.operationId(), source.batchId(), source.metadata(), source.captureId()
                )));
            }
            CaptureJournal.JournalBatch batch = journal.readBatch(8_192);
            assertTrue(batch.changes().size() < 20);
            assertTrue(batch.endOffset() - batch.startOffset() <= 16L * 1024 * 1024);
            int firstCount = batch.changes().size();
            journal.acknowledge(batch);
            assertEquals(20 - firstCount, journal.readBatch(8_192).changes().size());
        }
    }

    @Test
    void streamsALargeRecoveredBacklogWithMemoryBoundedByTheRequestedBatch() {
        Path file = temporaryDirectory.resolve("large-backlog.wal");
        int total = 25_000;
        try (CaptureJournal journal = new CaptureJournal(file)) {
            journal.open();
            for (int start = 0; start < total; start += 512) {
                List<ChangeRecord> page = new ArrayList<>();
                for (int index = start; index < Math.min(total, start + 512); index++) {
                    page.add(change(index));
                }
                journal.append(page);
            }
        }

        int replayed = 0;
        int maximumBatch = 0;
        try (CaptureJournal recovered = new CaptureJournal(file)) {
            recovered.open();
            while (recovered.pendingCount() > 0L) {
                CaptureJournal.JournalBatch batch = recovered.readBatch(257);
                assertTrue(batch.replayed());
                maximumBatch = Math.max(maximumBatch, batch.changes().size());
                replayed += batch.changes().size();
                recovered.acknowledge(batch);
            }
        }

        assertEquals(total, replayed);
        assertTrue(maximumBatch <= 257);
    }

    private static ChangeRecord change(int index) {
        return new ChangeRecord(
            0L,
            1_000L + index,
            new BlockPosition(
                UUID.fromString("10000000-0000-0000-0000-000000000001"),
                index,
                64,
                -index
            ),
            ActorRef.player(
                UUID.fromString("20000000-0000-0000-0000-000000000002"),
                "Builder"
            ),
            ChangeCause.WORLD_EDIT,
            BlockSnapshot.block("minecraft:stone"),
            new BlockSnapshot("minecraft:chest", "inventory/v1", new byte[] {1, 2, 3}),
            null,
            UUID.fromString("30000000-0000-0000-0000-000000000003"),
            "journal-" + index
        );
    }
}
