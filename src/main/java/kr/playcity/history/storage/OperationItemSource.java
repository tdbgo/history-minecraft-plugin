package kr.playcity.history.storage;

import kr.playcity.history.model.OperationItem;

import java.util.List;

/** Bounded, one-way source for a potentially large operation plan. */
public interface OperationItemSource extends AutoCloseable {
    List<OperationItem> readBatch(int maximumItems);

    @Override
    void close();
}
