package kr.playcity.history.storage;

import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeRecord;

import java.nio.charset.StandardCharsets;

/** Estimates un-normalized input bytes for relative category attribution. */
final class StorageFootprintEstimator {
    private static final long FIXED_RECORD_BYTES = 68L;

    private StorageFootprintEstimator() {
    }

    static long estimate(ChangeRecord change) {
        long bytes = FIXED_RECORD_BYTES;
        bytes = Math.addExact(bytes, utf8Length(change.actor().name()));
        bytes = Math.addExact(bytes, snapshotBytes(change.before()));
        bytes = Math.addExact(bytes, snapshotBytes(change.after()));
        bytes = Math.addExact(bytes, utf8Length(change.metadata()));
        if (change.operationId() != null) {
            bytes = Math.addExact(bytes, 16L);
        }
        if (change.batchId() != null) {
            bytes = Math.addExact(bytes, 16L);
        }
        return bytes;
    }

    private static long snapshotBytes(BlockSnapshot snapshot) {
        long bytes = utf8Length(snapshot.blockData());
        bytes = Math.addExact(bytes, utf8Length(snapshot.payloadType()));
        return Math.addExact(bytes, snapshot.payloadSize());
    }

    private static int utf8Length(String value) {
        return value.getBytes(StandardCharsets.UTF_8).length;
    }
}
