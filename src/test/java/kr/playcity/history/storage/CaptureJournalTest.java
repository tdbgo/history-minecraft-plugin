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
            journal.append(List.of(change(5)));
            CaptureJournal.JournalBatch firstAttempt = journal.readBatch(10);
            assertFalse(firstAttempt.replayed());

            journal.requireIdempotentRetry(firstAttempt);
            CaptureJournal.JournalBatch retry = journal.readBatch(10);
            assertTrue(retry.replayed());
            assertEquals(firstAttempt.changes(), retry.changes());
            journal.acknowledge(retry);
            assertEquals(0L, journal.pendingCount());
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
