package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Pure confirmation-time stale-preview validation. */
public final class LatestHistoryValidator {
    public void requireCurrent(
        List<OperationItem> items,
        Map<BlockPosition, LatestState> latestByPosition,
        OperationKind kind,
        java.util.UUID inverseOf,
        boolean includePayload
    ) {
        Objects.requireNonNull(items, "items");
        Objects.requireNonNull(latestByPosition, "latestByPosition");
        Objects.requireNonNull(kind, "kind");
        if (kind == OperationKind.UNDO) {
            Objects.requireNonNull(inverseOf, "inverseOf");
        }
        for (OperationItem item : items) {
            LatestState latest = latestByPosition.get(item.position());
            boolean sameSource = kind == OperationKind.ROLLBACK
                ? latest != null
                    && !item.sourceIds().isEmpty()
                    && latest.changeId() == item.sourceIds().getFirst()
                : latest != null && inverseOf.equals(latest.operationId());
            if (latest == null
                || !sameSource
                || !matches(latest.after(), item.before(), includePayload)) {
                throw new IllegalStateException(
                    "미리보기 이후 변경된 블록이 있어 작업을 중단했습니다. 새 미리보기를 만드세요."
                );
            }
        }
    }

    public record LatestState(long changeId, BlockSnapshot after, java.util.UUID operationId) {
        public LatestState {
            after = Objects.requireNonNull(after, "after");
            if (changeId <= 0L) {
                throw new IllegalArgumentException("changeId must be positive");
            }
        }
    }

    private static boolean matches(
        BlockSnapshot actual,
        BlockSnapshot expected,
        boolean includePayload
    ) {
        return actual.sameState(expected, includePayload);
    }
}
