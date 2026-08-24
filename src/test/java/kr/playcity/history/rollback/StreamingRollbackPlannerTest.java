package kr.playcity.history.rollback;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.OperationItem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StreamingRollbackPlannerTest {
    private static final UUID WORLD_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final ActorRef ACTOR = ActorRef.player(
        UUID.fromString("20000000-0000-0000-0000-000000000002"),
        "Builder"
    );

    @TempDir
    Path temporaryDirectory;

    @Test
    void plansAlternatingDirectAndWorldEditHistoryPastLegacyGlobalCaps() {
        int positions = 20_000;
        OperationPlanSpool.Writer writer = OperationPlanSpool.create(
            OperationPlanSpool.prepareDirectory(temporaryDirectory)
        );
        StreamingRollbackPlanner planner = new StreamingRollbackPlanner(
            new RequestedRollbackBoundary(WORLD_ID, positions / 2, 0, positions),
            -64,
            320,
            true,
            writer
        );
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000003");

        for (int x = 0; x < positions; x++) {
            BlockPosition position = new BlockPosition(WORLD_ID, x, 64, 0);
            planner.accept(change(
                x * 2L + 2L,
                2_000_000L + x,
                position,
                ChangeCause.WORLD_EDIT,
                "minecraft:dirt",
                "minecraft:bricks",
                batchId
            ));
            planner.accept(change(
                x * 2L + 1L,
                1_000_000L + x,
                position,
                ChangeCause.PLAYER_PLACE,
                "minecraft:air",
                "minecraft:dirt",
                null
            ));
        }

        StreamingRollbackPlanner.Result result = planner.finish();
        assertEquals(40_000L, result.sourceChanges());
        assertEquals(positions, result.candidateCount());
        assertEquals(1_250, result.chunkCount());
        assertEquals(0, result.conflicts());
        assertEquals(0, result.alreadyTarget());

        int streamed = 0;
        OperationItem first = null;
        OperationItem last = null;
        try (OperationPlanSpool.Reader reader = OperationPlanSpool.open(result.planFile())) {
            while (true) {
                List<OperationItem> batch = reader.readBatch(137);
                if (batch.isEmpty()) {
                    break;
                }
                if (first == null) {
                    first = batch.getFirst();
                }
                last = batch.getLast();
                assertTrue(batch.size() <= 137);
                streamed += batch.size();
            }
        }
        assertEquals(positions, streamed);
        assertEquals("minecraft:bricks", first.before().blockData());
        assertEquals("minecraft:air", first.after().blockData());
        assertEquals(positions - 1, last.position().x());
    }

    @Test
    void keepsOneHotCoordinateMemoryBoundedAcrossHundredsOfThousandsOfChanges() {
        int changes = 250_001;
        OperationPlanSpool.Writer writer = OperationPlanSpool.create(
            OperationPlanSpool.prepareDirectory(temporaryDirectory)
        );
        StreamingRollbackPlanner planner = new StreamingRollbackPlanner(
            new RequestedRollbackBoundary(WORLD_ID, 0, 0, 1),
            -64,
            320,
            false,
            writer
        );
        BlockPosition position = new BlockPosition(WORLD_ID, 0, 64, 0);
        for (int index = changes; index > 0; index--) {
            String before = (index & 1) == 0 ? "minecraft:stone" : "minecraft:dirt";
            String after = (index & 1) == 0 ? "minecraft:dirt" : "minecraft:stone";
            planner.accept(change(index, index, position, ChangeCause.PLAYER_PLACE, before, after, null));
        }

        StreamingRollbackPlanner.Result result = planner.finish();
        assertEquals(changes, result.sourceChanges());
        assertEquals(1, result.candidateCount());
        try (OperationPlanSpool.Reader reader = OperationPlanSpool.open(result.planFile())) {
            OperationItem item = reader.readBatch(1).getFirst();
            assertEquals(List.of((long) changes), item.sourceIds());
            assertTrue(reader.readBatch(1).isEmpty());
        }
    }

    private static ChangeRecord change(
        long id,
        long occurredAt,
        BlockPosition position,
        ChangeCause cause,
        String before,
        String after,
        UUID batchId
    ) {
        return new ChangeRecord(
            id,
            occurredAt,
            position,
            ACTOR,
            cause,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            batchId,
            ""
        );
    }
}
