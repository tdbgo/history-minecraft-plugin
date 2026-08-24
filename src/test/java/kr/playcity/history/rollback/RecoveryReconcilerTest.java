package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockSnapshot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RecoveryReconcilerTest {
    private final RecoveryReconciler reconciler = new RecoveryReconciler();
    private final BlockSnapshot before = BlockSnapshot.block("minecraft:stone");
    private final BlockSnapshot after = BlockSnapshot.block("minecraft:dirt");

    @Test
    void appliesOnlyWhenThePlannedBeforeStateIsStillLive() {
        assertEquals(
            RecoveryReconciler.Decision.APPLY,
            reconciler.decide(before, before, after, false)
        );
    }

    @Test
    void checkpointsWithoutReapplyingWhenTargetStateIsAlreadyLive() {
        assertEquals(
            RecoveryReconciler.Decision.ALREADY_APPLIED,
            reconciler.decide(after, before, after, false)
        );
    }

    @Test
    void refusesToOverwriteAThirdPartyState() {
        assertEquals(
            RecoveryReconciler.Decision.CONFLICT,
            reconciler.decide(BlockSnapshot.block("minecraft:gold_block"), before, after, false)
        );
    }
}
