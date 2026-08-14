package kr.playcity.history.storage;

import kr.playcity.history.model.ChangeCause;

import java.util.Objects;

public record CauseStorageMetric(
    ChangeCause cause,
    long changeCount,
    long estimatedInputBytes,
    long firstOccurredAt,
    long lastOccurredAt
) {
    public CauseStorageMetric {
        cause = Objects.requireNonNull(cause, "cause");
        if (changeCount < 0L || estimatedInputBytes < 0L) {
            throw new IllegalArgumentException("Storage metric counters must not be negative");
        }
    }

    public double estimatedBytesPerChange() {
        return changeCount == 0L ? 0.0D : (double) estimatedInputBytes / changeCount;
    }
}
