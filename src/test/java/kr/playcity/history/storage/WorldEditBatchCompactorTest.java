package kr.playcity.history.storage;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.RollbackPlan;
import kr.playcity.history.rollback.RollbackPlanner;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class WorldEditBatchCompactorTest {
    private static final UUID WORLD = UUID.fromString("70000000-0000-0000-0000-000000000007");
    private static final UUID BATCH = UUID.fromString("80000000-0000-0000-0000-000000000008");
    private static final ActorRef ACTOR = ActorRef.player(
        UUID.fromString("90000000-0000-0000-0000-000000000009"),
        "Builder"
    );
    private static final BlockPosition POSITION = new BlockPosition(WORLD, 8, 64, 8);

    @Test
    void mergesAContinuousTransitionInsideOneBatch() {
        List<ChangeRecord> compacted = WorldEditBatchCompactor.compact(List.of(
            worldEdit(1L, "minecraft:stone", "minecraft:dirt"),
            worldEdit(2L, "minecraft:dirt", "minecraft:gold_block")
        ));

        assertEquals(1, compacted.size());
        assertEquals("minecraft:stone", compacted.getFirst().before().blockData());
        assertEquals("minecraft:gold_block", compacted.getFirst().after().blockData());
        assertEquals(2L, compacted.getFirst().occurredAt());
    }

    @Test
    void removesAWithinBatchRoundTripWithNoNetWorldChange() {
        assertEquals(0, WorldEditBatchCompactor.compact(List.of(
            worldEdit(1L, "minecraft:stone", "minecraft:dirt"),
            worldEdit(2L, "minecraft:dirt", "minecraft:stone")
        )).size());
    }

    @Test
    void dropsDuplicateCaptureFromTwoFaweObservationPaths() {
        assertEquals(1, WorldEditBatchCompactor.compact(List.of(
            worldEdit(1L, "minecraft:stone", "minecraft:dirt"),
            worldEdit(2L, "minecraft:stone", "minecraft:dirt")
        )).size());
    }

    @Test
    void playerChangeAtTheSameCoordinateIsAnUncrossableBarrier() {
        ChangeRecord player = new ChangeRecord(
            0L,
            2L,
            POSITION,
            ACTOR,
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.block("minecraft:dirt"),
            BlockSnapshot.block("minecraft:oak_planks"),
            null,
            ""
        );

        List<ChangeRecord> compacted = WorldEditBatchCompactor.compact(List.of(
            worldEdit(1L, "minecraft:stone", "minecraft:dirt"),
            player,
            worldEdit(3L, "minecraft:oak_planks", "minecraft:gold_block")
        ));

        assertEquals(3, compacted.size());
        assertEquals(ChangeCause.PLAYER_PLACE, compacted.get(1).cause());
    }

    @Test
    void editsAtOtherCoordinatesDoNotPreventSafePerCoordinateCompaction() {
        ChangeRecord other = new ChangeRecord(
            0L,
            2L,
            new BlockPosition(WORLD, 9, 64, 8),
            ACTOR,
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.air(),
            BlockSnapshot.block("minecraft:oak_planks"),
            null,
            ""
        );

        List<ChangeRecord> compacted = WorldEditBatchCompactor.compact(List.of(
            worldEdit(1L, "minecraft:stone", "minecraft:dirt"),
            other,
            worldEdit(3L, "minecraft:dirt", "minecraft:gold_block")
        ));

        assertEquals(2, compacted.size());
        assertEquals("minecraft:gold_block", compacted.getFirst().after().blockData());
        assertEquals(ChangeCause.PLAYER_PLACE, compacted.get(1).cause());
    }

    @Test
    void compactedAlternatingBuildPatternStillRollsBackToItsExactInitialState() {
        UUID secondBatch = UUID.fromString("80000000-0000-0000-0000-000000000088");
        List<ChangeRecord> chronological = List.of(
            player(1L, "minecraft:air", "minecraft:stone"),
            worldEdit(2L, "minecraft:stone", "minecraft:dirt"),
            worldEdit(3L, "minecraft:dirt", "minecraft:bricks"),
            player(4L, "minecraft:bricks", "minecraft:polished_andesite"),
            worldEdit(5L, "minecraft:polished_andesite", "minecraft:quartz_block", secondBatch)
        );
        List<ChangeRecord> compacted = WorldEditBatchCompactor.compact(chronological);
        List<ChangeRecord> persistedNewestFirst = new java.util.ArrayList<>();
        for (int index = 0; index < compacted.size(); index++) {
            persistedNewestFirst.add(compacted.get(index).withId(index + 1L));
        }
        java.util.Collections.reverse(persistedNewestFirst);

        RollbackPlan plan = new RollbackPlanner().consolidate(persistedNewestFirst, true);

        assertEquals(4, compacted.size());
        assertEquals(0, plan.unsafePositionCount());
        assertEquals(1, plan.changes().size());
        assertEquals("minecraft:quartz_block", plan.changes().getFirst().expected().blockData());
        assertEquals("minecraft:air", plan.changes().getFirst().target().blockData());
        assertEquals(List.of(4L, 3L, 2L, 1L), plan.changes().getFirst().sourceIds());
    }

    private static ChangeRecord worldEdit(long time, String before, String after) {
        return worldEdit(time, before, after, BATCH);
    }

    private static ChangeRecord worldEdit(long time, String before, String after, UUID batchId) {
        return new ChangeRecord(
            0L,
            time,
            POSITION,
            ACTOR,
            ChangeCause.WORLD_EDIT,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            batchId,
            ""
        );
    }

    private static ChangeRecord player(long time, String before, String after) {
        return new ChangeRecord(
            0L,
            time,
            POSITION,
            ACTOR,
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            ""
        );
    }
}
