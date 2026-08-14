package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.PlannedBlockChange;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RollbackCandidateEvaluatorTest {
    private static final UUID WORLD = UUID.fromString("41000000-0000-0000-0000-000000000004");

    @Test
    void keepsDistantCandidatesWithoutConsultingChunkLoadState() {
        PlannedBlockChange near = change(1L, 0, 64, 0, "minecraft:stone", "minecraft:air");
        PlannedBlockChange far = change(
            2L,
            2_000_000,
            70,
            -2_000_000,
            "minecraft:diamond_block",
            "minecraft:air"
        );

        RollbackCandidateEvaluator.Result result = new RollbackCandidateEvaluator().evaluate(
            List.of(near, far),
            -64,
            320,
            false
        );

        assertEquals(2, result.candidates().size());
        assertEquals(far.position(), result.candidates().get(1).position());
        assertEquals(0, result.conflicts());
        assertEquals(0, result.alreadyTarget());
    }

    @Test
    void rejectsOnlyUnsafePayloadAndHeightItems() {
        PlannedBlockChange belowWorld = change(
            1L, 0, -65, 0, "minecraft:stone", "minecraft:air"
        );
        PlannedBlockChange payload = new PlannedBlockChange(
            new BlockPosition(WORLD, 1, 64, 1),
            new BlockSnapshot("minecraft:chest", "inventory/v1", new byte[] {1}),
            BlockSnapshot.air(),
            List.of(2L)
        );
        PlannedBlockChange noOp = change(
            3L, 2, 64, 2, "minecraft:stone", "minecraft:stone"
        );

        RollbackCandidateEvaluator.Result result = new RollbackCandidateEvaluator().evaluate(
            List.of(belowWorld, payload, noOp),
            -64,
            320,
            false
        );

        assertEquals(0, result.candidates().size());
        assertEquals(2, result.conflicts());
        assertEquals(1, result.alreadyTarget());
    }

    private static PlannedBlockChange change(
        long id,
        int x,
        int y,
        int z,
        String expected,
        String target
    ) {
        return new PlannedBlockChange(
            new BlockPosition(WORLD, x, y, z),
            BlockSnapshot.block(expected),
            BlockSnapshot.block(target),
            List.of(id)
        );
    }
}
