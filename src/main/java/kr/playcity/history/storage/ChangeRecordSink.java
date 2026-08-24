package kr.playcity.history.storage;

import kr.playcity.history.model.ChangeRecord;

@FunctionalInterface
public interface ChangeRecordSink {
    void accept(ChangeRecord change);
}
