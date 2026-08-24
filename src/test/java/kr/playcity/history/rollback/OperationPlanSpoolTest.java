package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OperationPlanSpoolTest {
    private static final UUID WORLD_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");

    @TempDir
    Path temporaryDirectory;

    @Test
    void roundTripsBoundedBatchesAndExactChunkGroups() {
        Path directory = OperationPlanSpool.prepareDirectory(temporaryDirectory);
        OperationItem first = item(0, 1, 64, 1);
        OperationItem second = item(1, 15, 65, 15);
        OperationItem distant = item(2, 16_000, 70, -16_000);
        OperationPlanSpool.Writer writer = OperationPlanSpool.create(directory);
        writer.write(first);
        writer.write(second);
        writer.write(distant);
        writer.close();

        try (OperationPlanSpool.Reader reader = OperationPlanSpool.open(writer.path())) {
            assertEquals(List.of(first, second), reader.readBatch(2));
            assertEquals(List.of(distant), reader.readBatch(2));
            assertTrue(reader.readBatch(2).isEmpty());
        }

        try (OperationPlanSpool.Reader reader = OperationPlanSpool.open(writer.path())) {
            OperationPlanSpool.PlanChunk nearby = reader.readChunk();
            OperationPlanSpool.PlanChunk far = reader.readChunk();
            assertEquals(List.of(first, second), nearby.items());
            assertEquals(ExactChunkCoordinate.from(first.position()), nearby.coordinate());
            assertEquals(List.of(distant), far.items());
            assertEquals(ExactChunkCoordinate.from(distant.position()), far.coordinate());
            assertEquals(null, reader.readChunk());
        }
    }

    @Test
    void rejectsATruncatedPlanBeforeReturningACompleteStream() throws Exception {
        Path directory = OperationPlanSpool.prepareDirectory(temporaryDirectory);
        OperationPlanSpool.Writer writer = OperationPlanSpool.create(directory);
        writer.write(item(0, 1, 64, 1));
        writer.close();
        byte[] complete = Files.readAllBytes(writer.path());
        Files.write(
            writer.path(),
            Arrays.copyOf(complete, complete.length - 5),
            StandardOpenOption.TRUNCATE_EXISTING
        );

        try (OperationPlanSpool.Reader reader = OperationPlanSpool.open(writer.path())) {
            assertEquals(1, reader.readBatch(1).size());
            assertThrows(IllegalStateException.class, () -> reader.readBatch(1));
        }
    }

    private static OperationItem item(int sequence, int x, int y, int z) {
        return new OperationItem(
            sequence,
            new BlockPosition(WORLD_ID, x, y, z),
            BlockSnapshot.block("minecraft:stone"),
            BlockSnapshot.block("minecraft:dirt"),
            List.of(100L + sequence, 50L + sequence)
        );
    }
}
