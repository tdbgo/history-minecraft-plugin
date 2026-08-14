package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.PlannedBlockChange;
import kr.playcity.history.model.RollbackPlan;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class RollbackPlanner {
    public RollbackPlan consolidate(List<ChangeRecord> newestFirst, boolean includePayload) {
        List<ChangeRecord> ordered = newestFirst.stream()
            .sorted(Comparator.comparingLong(ChangeRecord::occurredAt)
                .thenComparingLong(ChangeRecord::id)
                .reversed())
            .toList();
        Map<BlockPosition, Accumulator> byPosition = new LinkedHashMap<>();
        Map<Long, Accumulator> sourceIdOwners = new LinkedHashMap<>();
        for (ChangeRecord change : ordered) {
            if (!change.cause().rollbackEligible()) {
                continue;
            }
            BlockSnapshot after = payloadMode(change.after(), includePayload);
            BlockSnapshot before = payloadMode(change.before(), includePayload);
            Accumulator accumulator = byPosition.computeIfAbsent(
                change.position(),
                ignored -> new Accumulator(after, change.batchId())
            );
            Accumulator previousOwner = sourceIdOwners.putIfAbsent(change.id(), accumulator);
            if (change.id() <= 0L || previousOwner != null) {
                accumulator.unsafe = true;
                if (previousOwner != null) {
                    previousOwner.unsafe = true;
                }
                continue;
            }
            accumulator.sourceIds.add(change.id());
            if (accumulator.first) {
                accumulator.target = before;
                accumulator.lastBefore = before;
                accumulator.lastAfter = after;
                accumulator.first = false;
                continue;
            }
            if (isDuplicateBatchCapture(accumulator, change, before, after, includePayload)) {
                continue;
            }
            if (!accumulator.target.sameState(after, includePayload)) {
                accumulator.unsafe = true;
                continue;
            }
            accumulator.target = before;
            accumulator.lastBatchId = change.batchId();
            accumulator.lastBefore = before;
            accumulator.lastAfter = after;
        }

        List<PlannedBlockChange> planned = new ArrayList<>(byPosition.size());
        int unsafePositions = 0;
        for (Map.Entry<BlockPosition, Accumulator> entry : byPosition.entrySet()) {
            Accumulator accumulator = entry.getValue();
            if (accumulator.unsafe) {
                unsafePositions++;
                continue;
            }
            if (!accumulator.expected.sameState(accumulator.target, includePayload)) {
                planned.add(new PlannedBlockChange(
                    entry.getKey(),
                    accumulator.expected,
                    accumulator.target,
                    accumulator.sourceIds
                ));
            }
        }
        return new RollbackPlan(planned, newestFirst.size(), unsafePositions);
    }

    private static boolean isDuplicateBatchCapture(
        Accumulator accumulator,
        ChangeRecord change,
        BlockSnapshot before,
        BlockSnapshot after,
        boolean includePayload
    ) {
        UUID batchId = change.batchId();
        return batchId != null
            && batchId.equals(accumulator.lastBatchId)
            && before.sameState(accumulator.lastBefore, includePayload)
            && after.sameState(accumulator.lastAfter, includePayload);
    }

    private static BlockSnapshot payloadMode(BlockSnapshot snapshot, boolean includePayload) {
        return includePayload ? snapshot : snapshot.withoutPayload();
    }

    private static final class Accumulator {
        private final BlockSnapshot expected;
        private final List<Long> sourceIds = new ArrayList<>();
        private BlockSnapshot target;
        private UUID lastBatchId;
        private BlockSnapshot lastBefore;
        private BlockSnapshot lastAfter;
        private boolean first = true;
        private boolean unsafe;

        private Accumulator(BlockSnapshot expected, UUID batchId) {
            this.expected = expected;
            this.target = expected;
            this.lastBatchId = batchId;
            this.lastBefore = expected;
            this.lastAfter = expected;
        }
    }
}
