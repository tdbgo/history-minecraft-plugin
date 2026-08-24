package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockSnapshot;

import java.util.Objects;

/** Pure decision used when resuming a block whose checkpoint outcome is unknown. */
final class RecoveryReconciler {
    enum Decision {
        APPLY,
        ALREADY_APPLIED,
        CONFLICT
    }

    Decision decide(
        BlockSnapshot current,
        BlockSnapshot plannedBefore,
        BlockSnapshot plannedAfter,
        boolean comparePayload
    ) {
        Objects.requireNonNull(current, "current");
        Objects.requireNonNull(plannedBefore, "plannedBefore");
        Objects.requireNonNull(plannedAfter, "plannedAfter");
        if (current.sameState(plannedAfter, comparePayload)) {
            return Decision.ALREADY_APPLIED;
        }
        if (current.sameState(plannedBefore, comparePayload)) {
            return Decision.APPLY;
        }
        return Decision.CONFLICT;
    }
}
