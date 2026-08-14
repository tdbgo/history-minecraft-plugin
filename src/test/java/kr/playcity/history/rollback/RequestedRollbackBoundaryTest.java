package kr.playcity.history.rollback;

import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RequestedRollbackBoundaryTest {
    private static final UUID WORLD = UUID.fromString("a0000000-0000-0000-0000-00000000000a");

    @Test
    void acceptsOnlyTheRequestedCircularAreaInOneWorld() {
        RequestedRollbackBoundary boundary = new RequestedRollbackBoundary(WORLD, 100, -100, 10);

        assertTrue(boundary.contains(position(106, -92)));
        assertTrue(boundary.contains(position(110, -100)));
        assertFalse(boundary.contains(position(110, -99)));
        assertFalse(boundary.contains(position(111, -100)));
        assertFalse(boundary.contains(new BlockPosition(UUID.randomUUID(), 100, 64, -100)));
    }

    @Test
    void usesLongArithmeticAtExtremeCoordinates() {
        RequestedRollbackBoundary boundary = new RequestedRollbackBoundary(
            WORLD,
            Integer.MAX_VALUE,
            Integer.MIN_VALUE,
            100
        );

        assertTrue(boundary.contains(new BlockPosition(
            WORLD,
            Integer.MAX_VALUE - 60,
            64,
            Integer.MIN_VALUE + 80
        )));
        assertFalse(boundary.contains(new BlockPosition(WORLD, Integer.MIN_VALUE, 64, Integer.MAX_VALUE)));
    }

    @Test
    void supportsAMultiMillionBlockRequestedCircleWithoutExpandingMembership() {
        RequestedRollbackBoundary boundary = new RequestedRollbackBoundary(WORLD, 0, 0, 5_000_000);

        assertTrue(boundary.contains(position(3_000_000, -3_000_000)));
        assertFalse(boundary.contains(position(5_000_000, 1)));
        assertFalse(boundary.contains(position(5_000_001, 0)));
    }

    @Test
    void failsClosedWhenARepositoryReturnsOneOutOfScopeRecord() {
        RequestedRollbackBoundary boundary = new RequestedRollbackBoundary(WORLD, 0, 0, 5);
        ChangeRecord inside = change(position(3, 4));
        ChangeRecord outside = change(position(6, 0));

        assertDoesNotThrow(() -> boundary.requireContainsAll(List.of(inside)));
        assertThrows(IllegalStateException.class, () ->
            boundary.requireContainsAll(List.of(inside, outside)));
    }

    private static BlockPosition position(int x, int z) {
        return new BlockPosition(WORLD, x, 64, z);
    }

    private static ChangeRecord change(BlockPosition position) {
        return new ChangeRecord(
            1L,
            1L,
            position,
            ActorRef.system("#test"),
            ChangeCause.PLAYER_PLACE,
            BlockSnapshot.air(),
            BlockSnapshot.block("minecraft:stone"),
            null,
            ""
        );
    }
}
