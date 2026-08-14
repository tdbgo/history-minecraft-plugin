package kr.playcity.history.model;

import java.util.Objects;

public record AppliedOperationItem(
    OperationItem item,
    BlockSnapshot actualBefore,
    BlockSnapshot actualAfter
) {
    public AppliedOperationItem {
        item = Objects.requireNonNull(item, "item");
        actualBefore = Objects.requireNonNull(actualBefore, "actualBefore");
        actualAfter = Objects.requireNonNull(actualAfter, "actualAfter");
    }
}
