package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Watches only coordinates belonging to active rollback confirmations.
 *
 * <p>The recorder executes its queue append inside the same per-key map
 * operation that advances the generation. Therefore a mutation is either
 * persisted before a watch snapshot (and caught by latest-history validation)
 * or advances the generation after the snapshot (and is caught here).</p>
 */
public final class ActivePositionGuard {
    private final ConcurrentHashMap<BlockPosition, Slot> active = new ConcurrentHashMap<>();

    public <T> T recordMutation(BlockPosition position, Supplier<T> recorder) {
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(recorder, "recorder");
        Holder<T> result = new Holder<>();
        active.compute(position, (ignored, slot) -> {
            if (slot != null) {
                slot.generation++;
            }
            result.value = recorder.get();
            return slot;
        });
        return result.value;
    }

    /** Marks a mutation boundary without performing queue work. */
    public void markMutation(BlockPosition position) {
        Objects.requireNonNull(position, "position");
        active.computeIfPresent(position, (ignored, slot) -> {
            slot.generation++;
            return slot;
        });
    }

    public Watch watch(Collection<BlockPosition> positions) {
        Objects.requireNonNull(positions, "positions");
        Set<BlockPosition> unique = new HashSet<>(positions);
        if (unique.isEmpty()) {
            throw new IllegalArgumentException("A position watch must not be empty");
        }
        if (unique.size() != positions.size()) {
            throw new IllegalArgumentException("A position watch must not contain duplicates");
        }

        Map<BlockPosition, Long> baseline = new HashMap<>();
        try {
            for (BlockPosition position : unique) {
                Objects.requireNonNull(position, "watched position");
                Slot slot = active.compute(position, (ignored, existing) -> {
                    Slot value = existing == null ? new Slot() : existing;
                    value.references++;
                    return value;
                });
                baseline.put(position, slot.generation);
            }
            return new Watch(this, baseline);
        } catch (RuntimeException failure) {
            release(baseline.keySet());
            throw failure;
        }
    }

    int watchedPositionCount() {
        return active.size();
    }

    private void requireUnchanged(BlockPosition position, long expectedGeneration) {
        Slot slot = active.get(position);
        if (slot == null || slot.generation != expectedGeneration) {
            throw new IllegalStateException(
                "복구 확인 이후 대상 블록이 다시 변경되어 작업을 중단했습니다. 새 미리보기를 만드세요."
            );
        }
    }

    private void release(Collection<BlockPosition> positions) {
        for (BlockPosition position : positions) {
            active.computeIfPresent(position, (ignored, slot) -> {
                slot.references--;
                if (slot.references < 0) {
                    throw new IllegalStateException("Position watch reference count became negative");
                }
                return slot.references == 0 ? null : slot;
            });
        }
    }

    public static final class Watch implements AutoCloseable {
        private final ActivePositionGuard owner;
        private final Map<BlockPosition, Long> baseline;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Watch(ActivePositionGuard owner, Map<BlockPosition, Long> baseline) {
            this.owner = owner;
            this.baseline = Map.copyOf(baseline);
        }

        public void requireUnchanged(BlockPosition position) {
            Objects.requireNonNull(position, "position");
            if (closed.get()) {
                throw new IllegalStateException("Position watch is already closed");
            }
            Long expected = baseline.get(position);
            if (expected == null) {
                throw new IllegalStateException("Rollback attempted to check a position outside its watch");
            }
            owner.requireUnchanged(position, expected);
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                owner.release(baseline.keySet());
            }
        }
    }

    private static final class Slot {
        private volatile long generation;
        private int references;
    }

    private static final class Holder<T> {
        private T value;
    }
}
