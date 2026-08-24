package kr.playcity.history.storage;

import kr.playcity.history.model.OperationItem;

@FunctionalInterface
public interface OperationItemSink {
    void accept(OperationItem item);
}
