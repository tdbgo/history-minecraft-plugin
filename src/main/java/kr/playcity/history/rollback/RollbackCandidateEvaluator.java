package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.PlannedBlockChange;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Builds preview candidates from the persisted, continuity-checked history.
 *
 * <p>Chunk load state is intentionally absent from this domain boundary. A
 * distant or currently unloaded chunk remains a candidate and is revalidated
 * against the live block only after an exact chunk lease is acquired.</p>
 */
public final class RollbackCandidateEvaluator {
    public Result evaluate(
        List<PlannedBlockChange> planned,
        int minimumHeight,
        int maximumHeight,
        boolean restoreBlockEntityData
    ) {
        Objects.requireNonNull(planned, "planned");
        if (minimumHeight >= maximumHeight) {
            throw new IllegalArgumentException("World height bounds are invalid");
        }

        List<OperationItem> candidates = new ArrayList<>();
        int conflicts = 0;
        int alreadyTarget = 0;
        for (PlannedBlockChange change : planned) {
            Objects.requireNonNull(change, "planned change");
            if (change.position().y() < minimumHeight || change.position().y() >= maximumHeight) {
                conflicts++;
                continue;
            }
            if (!restoreBlockEntityData
                && (change.expected().hasPayload() || change.target().hasPayload())) {
                conflicts++;
                continue;
            }
            BlockSnapshot expected = payloadMode(change.expected(), restoreBlockEntityData);
            BlockSnapshot target = payloadMode(change.target(), restoreBlockEntityData);
            if (expected.sameState(target, restoreBlockEntityData)) {
                alreadyTarget++;
                continue;
            }
            candidates.add(new OperationItem(
                candidates.size(),
                change.position(),
                expected,
                target,
                change.sourceIds()
            ));
        }
        return new Result(candidates, conflicts, alreadyTarget);
    }

    private static BlockSnapshot payloadMode(BlockSnapshot snapshot, boolean restoreBlockEntityData) {
        return restoreBlockEntityData ? snapshot : snapshot.withoutPayload();
    }

    public record Result(List<OperationItem> candidates, int conflicts, int alreadyTarget) {
        public Result {
            candidates = List.copyOf(candidates);
            if (conflicts < 0 || alreadyTarget < 0) {
                throw new IllegalArgumentException("Candidate counters must not be negative");
            }
        }
    }
}
