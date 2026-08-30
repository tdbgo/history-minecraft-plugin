package kr.playcity.history.storage;

/** Immutable diagnostic snapshot. Storage backlog and external callbacks are separate. */
public record StoreStatus(
    String backend,
    boolean ready,
    boolean accepting,
    boolean healthy,
    boolean degraded,
    int databaseQueued,
    int volatileQueued,
    long durableJournalBytes,
    int pendingReservations,
    long pendingReservationChanges,
    long oldestReservationAgeMillis,
    String oldestReservationId,
    long accepted,
    long persisted,
    long compacted,
    long rejected,
    long captureGapEvents,
    long captureGapChanges,
    long unknownCaptureGapEvents,
    long worldEditCaptureGapEvents,
    long worldEditCaptureGapChanges,
    long purged,
    int interruptedOperations,
    String lastError
) {
    /** Compatibility constructor for alpha.7 diagnostics without journal fields. */
    public StoreStatus(
        String backend,
        boolean ready,
        boolean accepting,
        boolean healthy,
        boolean degraded,
        int databaseQueued,
        int pendingReservations,
        long pendingReservationChanges,
        long oldestReservationAgeMillis,
        String oldestReservationId,
        long accepted,
        long persisted,
        long compacted,
        long rejected,
        long captureGapEvents,
        long captureGapChanges,
        long unknownCaptureGapEvents,
        long worldEditCaptureGapEvents,
        long worldEditCaptureGapChanges,
        long purged,
        int interruptedOperations,
        String lastError
    ) {
        this(
            backend, ready, accepting, healthy, degraded, databaseQueued, 0, 0L,
            pendingReservations, pendingReservationChanges, oldestReservationAgeMillis, oldestReservationId,
            accepted, persisted, compacted, rejected, captureGapEvents, captureGapChanges,
            unknownCaptureGapEvents, worldEditCaptureGapEvents, worldEditCaptureGapChanges,
            purged, interruptedOperations, lastError
        );
    }

    /** Compatibility constructor used by small test stores and older adapters. */
    public StoreStatus(
        String backend,
        boolean ready,
        boolean accepting,
        boolean healthy,
        boolean captureComplete,
        int queued,
        long accepted,
        long persisted,
        long compacted,
        long rejected,
        long blockedWorldEdits,
        long purged,
        int interruptedOperations,
        String lastError
    ) {
        this(
            backend, ready, accepting, healthy, !captureComplete, queued, 0, 0L,
            0, 0L, 0L, "", accepted, persisted, compacted, rejected,
            captureComplete ? 0L : Math.max(1L, rejected), rejected, 0L,
            blockedWorldEdits, blockedWorldEdits, purged, interruptedOperations, lastError
        );
    }

    /** True only when no capture gap has been observed in this process. */
    public boolean captureComplete() {
        return captureGapEvents == 0L;
    }

    /** Legacy aggregate; new UI should show the two components separately. */
    public int queued() {
        long total = (long) volatileQueued + databaseQueued + pendingReservationChanges;
        return (int) Math.min(Integer.MAX_VALUE, total);
    }

    /** Formerly meant edits blocked before application; now counts non-blocking capture gaps. */
    public long blockedWorldEdits() {
        return worldEditCaptureGapEvents;
    }

    public boolean operational() {
        return ready && accepting && healthy && !degraded;
    }

    public boolean recoveryAvailable() {
        return ready && accepting && healthy && degraded
            && volatileQueued == 0 && databaseQueued == 0
            && pendingReservations == 0 && pendingReservationChanges == 0L;
    }
}
