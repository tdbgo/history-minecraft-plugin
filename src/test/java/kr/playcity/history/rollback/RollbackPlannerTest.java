package kr.playcity.history.rollback;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.RollbackPlan;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RollbackPlannerTest {
    private static final UUID WORLD_ID = UUID.fromString("10000000-0000-0000-0000-000000000001");
    private static final UUID ACTOR_ID = UUID.fromString("20000000-0000-0000-0000-000000000002");
    private static final ActorRef ACTOR = ActorRef.player(ACTOR_ID, "Builder");
    private static final BlockPosition POSITION = new BlockPosition(WORLD_ID, 10, 64, 20);

    @Test
    void consolidatesNewestFirstHistoryToTheEarliestBeforeState() {
        ChangeRecord older = change(1L, 100L, "minecraft:stone", "minecraft:dirt");
        ChangeRecord newer = change(2L, 200L, "minecraft:dirt", "minecraft:gold_block");

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(newer, older), false);

        assertEquals(2, plan.sourceChangeCount());
        assertEquals(1, plan.changes().size());
        assertEquals("minecraft:gold_block", plan.changes().getFirst().expected().blockData());
        assertEquals("minecraft:stone", plan.changes().getFirst().target().blockData());
        assertEquals(List.of(2L, 1L), plan.changes().getFirst().sourceIds());
        assertEquals(0, plan.unsafePositionCount());
    }

    @Test
    void removesASequenceWhoseNetStateIsUnchanged() {
        ChangeRecord older = change(1L, 100L, "minecraft:stone", "minecraft:dirt");
        ChangeRecord newer = change(2L, 200L, "minecraft:dirt", "minecraft:stone");

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(newer, older), false);

        assertTrue(plan.changes().isEmpty());
        assertEquals(2, plan.sourceChangeCount());
    }

    @Test
    void excludesBlockEntityPayloadWhenRestorationIsDisabled() {
        BlockSnapshot before = new BlockSnapshot("minecraft:chest", "inventory/v1", new byte[] {1, 2});
        BlockSnapshot after = new BlockSnapshot("minecraft:chest", "inventory/v1", new byte[] {3, 4});
        ChangeRecord record = new ChangeRecord(
            3L,
            300L,
            POSITION,
            ACTOR,
            ChangeCause.PLAYER_BREAK,
            before,
            after,
            null,
            ""
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(record), false);

        assertTrue(plan.changes().isEmpty());
    }

    @Test
    void plansAWorldEditBatchForRollback() {
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000003");
        ChangeRecord record = new ChangeRecord(
            4L,
            400L,
            POSITION,
            ACTOR,
            ChangeCause.WORLD_EDIT,
            BlockSnapshot.block("minecraft:stone"),
            BlockSnapshot.block("minecraft:emerald_block"),
            null,
            batchId,
            ""
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(record), false);

        assertEquals(1, plan.sourceChangeCount());
        assertEquals(1, plan.changes().size());
        assertEquals("minecraft:emerald_block", plan.changes().getFirst().expected().blockData());
        assertEquals("minecraft:stone", plan.changes().getFirst().target().blockData());
        assertEquals(List.of(4L), plan.changes().getFirst().sourceIds());
    }

    @Test
    void safelyConsolidatesAlternatingPlayerAndWorldEditBuildingChanges() {
        BlockPosition wall = POSITION;
        BlockPosition doorway = new BlockPosition(WORLD_ID, 11, 64, 20);
        UUID firstEdit = UUID.fromString("30000000-0000-0000-0000-000000000031");
        UUID secondEdit = UUID.fromString("30000000-0000-0000-0000-000000000032");

        List<ChangeRecord> interleavedAndUnsorted = List.of(
            change(2L, 120L, wall, ChangeCause.WORLD_EDIT,
                "minecraft:stone", "minecraft:bricks", firstEdit),
            change(7L, 230L, doorway, ChangeCause.PLAYER_BREAK,
                "minecraft:oak_planks", "minecraft:air", null),
            change(1L, 100L, wall, ChangeCause.PLAYER_PLACE,
                "minecraft:air", "minecraft:stone", null),
            change(8L, 240L, doorway, ChangeCause.WORLD_EDIT,
                "minecraft:air", "minecraft:oak_door[facing=north,half=lower]", secondEdit),
            change(4L, 220L, wall, ChangeCause.WORLD_EDIT,
                "minecraft:polished_andesite", "minecraft:quartz_block", secondEdit),
            change(5L, 110L, doorway, ChangeCause.PLAYER_PLACE,
                "minecraft:air", "minecraft:oak_planks", null),
            change(3L, 200L, wall, ChangeCause.PLAYER_PLACE,
                "minecraft:bricks", "minecraft:polished_andesite", null),
            change(6L, 130L, doorway, ChangeCause.WORLD_EDIT,
                "minecraft:oak_planks", "minecraft:spruce_planks", firstEdit),
            change(9L, 210L, doorway, ChangeCause.PLAYER_PLACE,
                "minecraft:spruce_planks", "minecraft:oak_planks", null)
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(interleavedAndUnsorted, true);

        assertEquals(9, plan.sourceChangeCount());
        assertEquals(0, plan.unsafePositionCount());
        assertEquals(2, plan.changes().size());
        Map<BlockPosition, kr.playcity.history.model.PlannedBlockChange> byPosition = plan.changes().stream()
            .collect(Collectors.toMap(
                kr.playcity.history.model.PlannedBlockChange::position,
                Function.identity()
            ));
        assertEquals("minecraft:quartz_block", byPosition.get(wall).expected().blockData());
        assertEquals("minecraft:air", byPosition.get(wall).target().blockData());
        assertEquals(List.of(4L, 3L, 2L, 1L), byPosition.get(wall).sourceIds());
        assertEquals(
            "minecraft:oak_door[facing=north,half=lower]",
            byPosition.get(doorway).expected().blockData()
        );
        assertEquals("minecraft:air", byPosition.get(doorway).target().blockData());
        assertEquals(List.of(8L, 7L, 9L, 6L, 5L), byPosition.get(doorway).sourceIds());
    }

    @Test
    void rejectsAStateChainWithAnUnselectedInterveningChange() {
        ChangeRecord selectedOlder = change(
            10L, 100L, POSITION, ChangeCause.PLAYER_PLACE,
            "minecraft:stone", "minecraft:dirt", null
        );
        ChangeRecord selectedNewer = change(
            12L, 300L, POSITION, ChangeCause.WORLD_EDIT,
            "minecraft:gold_block", "minecraft:diamond_block", UUID.randomUUID()
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(selectedOlder, selectedNewer), true);

        assertTrue(plan.changes().isEmpty());
        assertEquals(1, plan.unsafePositionCount());
    }

    @Test
    void coalescesDuplicateCaptureFromTheSameWorldEditBatch() {
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000033");
        ChangeRecord firstCapture = change(
            20L, 100L, POSITION, ChangeCause.WORLD_EDIT,
            "minecraft:stone", "minecraft:bricks", batchId
        );
        ChangeRecord duplicateCapture = change(
            21L, 101L, POSITION, ChangeCause.WORLD_EDIT,
            "minecraft:stone", "minecraft:bricks", batchId
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(
            List.of(firstCapture, duplicateCapture),
            true
        );

        assertEquals(0, plan.unsafePositionCount());
        assertEquals(1, plan.changes().size());
        assertEquals("minecraft:bricks", plan.changes().getFirst().expected().blockData());
        assertEquals("minecraft:stone", plan.changes().getFirst().target().blockData());
        assertEquals(List.of(21L, 20L), plan.changes().getFirst().sourceIds());
    }

    @Test
    void preservesCaptureOrderInsideOneWorldEditBatchWhenIdsBreakTimestampTies() {
        UUID batchId = UUID.fromString("30000000-0000-0000-0000-000000000034");
        ChangeRecord first = change(
            30L, 500L, POSITION, ChangeCause.WORLD_EDIT,
            "minecraft:stone", "minecraft:dirt", batchId
        );
        ChangeRecord second = change(
            31L, 500L, POSITION, ChangeCause.WORLD_EDIT,
            "minecraft:dirt", "minecraft:gold_block", batchId
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(first, second), true);

        assertEquals(0, plan.unsafePositionCount());
        assertEquals("minecraft:gold_block", plan.changes().getFirst().expected().blockData());
        assertEquals("minecraft:stone", plan.changes().getFirst().target().blockData());
        assertEquals(List.of(31L, 30L), plan.changes().getFirst().sourceIds());
    }

    @Test
    void ignoresAuditOnlyEventsDefensively() {
        ChangeRecord audit = new ChangeRecord(
            5L,
            500L,
            POSITION,
            ACTOR,
            ChangeCause.PLAYER_COMMAND,
            BlockSnapshot.air(),
            BlockSnapshot.air(),
            null,
            "command:/history status"
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(List.of(audit), true);

        assertTrue(plan.changes().isEmpty());
    }

    private static ChangeRecord change(long id, long time, String before, String after) {
        return change(id, time, POSITION, ChangeCause.PLAYER_PLACE, before, after, null);
    }

    private static ChangeRecord change(
        long id,
        long time,
        BlockPosition position,
        ChangeCause cause,
        String before,
        String after,
        UUID batchId
    ) {
        return new ChangeRecord(
            id,
            time,
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
