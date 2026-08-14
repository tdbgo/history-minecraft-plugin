package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ActivePositionGuardTest {
    private static final UUID WORLD = UUID.fromString("42000000-0000-0000-0000-000000000004");
    private static final BlockPosition TARGET = new BlockPosition(WORLD, 1, 64, 1);

    @Test
    void detectsARecordedMutationForEveryOverlappingWatch() {
        ActivePositionGuard guard = new ActivePositionGuard();
        ActivePositionGuard.Watch first = guard.watch(List.of(TARGET));
        ActivePositionGuard.Watch second = guard.watch(List.of(TARGET));

        assertEquals("accepted", guard.recordMutation(TARGET, () -> "accepted"));
        assertThrows(IllegalStateException.class, () -> first.requireUnchanged(TARGET));
        assertThrows(IllegalStateException.class, () -> second.requireUnchanged(TARGET));

        first.close();
        assertEquals(1, guard.watchedPositionCount());
        second.close();
        assertEquals(0, guard.watchedPositionCount());
    }

    @Test
    void ignoresOtherCoordinatesAndReleasesIdempotently() {
        ActivePositionGuard guard = new ActivePositionGuard();
        BlockPosition other = new BlockPosition(WORLD, 2, 64, 1);
        ActivePositionGuard.Watch watch = guard.watch(List.of(TARGET));

        guard.recordMutation(other, () -> true);
        watch.requireUnchanged(TARGET);
        assertThrows(IllegalStateException.class, () -> watch.requireUnchanged(other));

        watch.close();
        watch.close();
        assertEquals(0, guard.watchedPositionCount());
        assertThrows(IllegalStateException.class, () -> watch.requireUnchanged(TARGET));
    }

    @Test
    void mutationCompletedBeforeWatchBecomesTheBaseline() {
        ActivePositionGuard guard = new ActivePositionGuard();
        guard.recordMutation(TARGET, () -> true);

        try (ActivePositionGuard.Watch watch = guard.watch(List.of(TARGET))) {
            watch.requireUnchanged(TARGET);
        }
    }

    @Test
    void rejectsDuplicateWatchCoordinates() {
        ActivePositionGuard guard = new ActivePositionGuard();
        assertThrows(IllegalArgumentException.class, () -> guard.watch(List.of(TARGET, TARGET)));
        assertEquals(0, guard.watchedPositionCount());
    }
}
