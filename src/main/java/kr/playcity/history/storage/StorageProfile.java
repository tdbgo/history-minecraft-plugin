package kr.playcity.history.storage;

import java.util.List;
import java.util.Objects;

public record StorageProfile(
    String backend,
    long databaseBytes,
    long auxiliaryBytes,
    List<CauseStorageMetric> metrics
) {
    public StorageProfile {
        backend = Objects.requireNonNull(backend, "backend");
        metrics = List.copyOf(metrics);
        if (databaseBytes < 0L || auxiliaryBytes < 0L) {
            throw new IllegalArgumentException("Storage sizes must not be negative");
        }
    }

    public long totalBytes() {
        return Math.addExact(databaseBytes, auxiliaryBytes);
    }
}
