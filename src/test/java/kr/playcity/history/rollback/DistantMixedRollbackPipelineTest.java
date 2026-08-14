package kr.playcity.history.rollback;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;
import kr.playcity.history.model.RollbackPlan;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DistantMixedRollbackPipelineTest {
    private static final UUID WORLD = UUID.fromString("43000000-0000-0000-0000-000000000004");
    private static final ActorRef BUILDER = ActorRef.player(
        UUID.fromString("43000000-0000-0000-0000-000000000005"),
        "Builder"
    );
    private static final BlockPosition NEAR = new BlockPosition(WORLD, 5, 64, 5);
    private static final BlockPosition FAR = new BlockPosition(WORLD, 3_000_000, 80, -3_000_000);

    @Test
    void preservesAlternatingDirectAndWorldEditChainsAcrossDistantChunks() {
        UUID firstBatch = UUID.fromString("43000000-0000-0000-0000-000000000010");
        UUID secondBatch = UUID.fromString("43000000-0000-0000-0000-000000000011");
        List<ChangeRecord> unsorted = List.of(
            change(6L, 600L, FAR, ChangeCause.WORLD_EDIT,
                "minecraft:bricks", "minecraft:diamond_block", secondBatch),
            change(2L, 200L, NEAR, ChangeCause.WORLD_EDIT,
                "minecraft:stone", "minecraft:quartz_block", firstBatch),
            change(4L, 400L, FAR, ChangeCause.WORLD_EDIT,
                "minecraft:air", "minecraft:dirt", firstBatch),
            change(1L, 100L, NEAR, ChangeCause.PLAYER_PLACE,
                "minecraft:air", "minecraft:stone", null),
            change(5L, 500L, FAR, ChangeCause.PLAYER_PLACE,
                "minecraft:dirt", "minecraft:bricks", null),
            change(3L, 300L, NEAR, ChangeCause.PLAYER_PLACE,
                "minecraft:quartz_block", "minecraft:gold_block", null)
        );

        RollbackPlan plan = new RollbackPlanner().consolidate(unsorted, false);
        RollbackCandidateEvaluator.Result evaluated = new RollbackCandidateEvaluator().evaluate(
            plan.changes(), -64, 320, false
        );
        ExactMutationScope scope = ExactMutationScope.create(evaluated.candidates(), 10, 2);

        assertEquals(0, plan.unsafePositionCount());
        assertEquals(2, scope.blockCount());
        assertEquals(2, scope.chunkCount());
        assertEquals(
            List.of(ExactChunkCoordinate.from(NEAR), ExactChunkCoordinate.from(FAR)),
            scope.chunks()
        );

        Map<BlockPosition, LatestHistoryValidator.LatestState> latest = new HashMap<>();
        latest.put(NEAR, new LatestHistoryValidator.LatestState(
            3L, BlockSnapshot.block("minecraft:gold_block"), null
        ));
        latest.put(FAR, new LatestHistoryValidator.LatestState(
            6L, BlockSnapshot.block("minecraft:diamond_block"), null
        ));
        LatestHistoryValidator validator = new LatestHistoryValidator();
        for (ExactChunkCoordinate chunk : scope.chunks()) {
            validator.requireCurrent(
                scope.itemsIn(chunk),
                latest,
                OperationKind.ROLLBACK,
                null,
                false
            );
        }

        ActivePositionGuard guard = new ActivePositionGuard();
        try (ActivePositionGuard.Watch watch = guard.watch(
            evaluated.candidates().stream().map(OperationItem::position).toList()
        )) {
            watch.requireUnchanged(FAR);
            guard.recordMutation(NEAR, () -> true);
            assertThrows(IllegalStateException.class, () -> watch.requireUnchanged(NEAR));
            watch.requireUnchanged(FAR);
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
            BUILDER,
            cause,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            null,
            batchId,
            ""
        );
    }
}
