package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.model.OperationKind;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LatestHistoryValidatorTest {
    private static final UUID WORLD = UUID.fromString("b0000000-0000-0000-0000-00000000000b");
    private static final BlockPosition POSITION = new BlockPosition(WORLD, 1, 64, 1);

    @Test
    void acceptsTheLatestPersistedStateMatchingThePreview() {
        OperationItem item = item("minecraft:gold_block");
        LatestHistoryValidator.LatestState latest = latest(1L, "minecraft:gold_block");

        assertDoesNotThrow(() -> new LatestHistoryValidator().requireCurrent(
            List.of(item),
            Map.of(POSITION, latest),
            OperationKind.ROLLBACK,
            null,
            true
        ));
    }

    @Test
    void rejectsAnInterveningManualOrWorldEditChangeEvenIfTheWorldLaterLooksSimilar() {
        OperationItem item = item("minecraft:gold_block");
        LatestHistoryValidator.LatestState intervening = latest(2L, "minecraft:diamond_block");

        assertThrows(IllegalStateException.class, () -> new LatestHistoryValidator().requireCurrent(
            List.of(item),
            Map.of(POSITION, intervening),
            OperationKind.ROLLBACK,
            null,
            true
        ));
        assertThrows(IllegalStateException.class, () -> new LatestHistoryValidator().requireCurrent(
            List.of(item),
            Map.of(),
            OperationKind.ROLLBACK,
            null,
            true
        ));
    }

    @Test
    void rejectsAChangedThenReturnedStateByItsNewerSourceId() {
        OperationItem item = item("minecraft:gold_block");
        LatestHistoryValidator.LatestState visuallySameButNewer = latest(3L, "minecraft:gold_block");

        assertThrows(IllegalStateException.class, () -> new LatestHistoryValidator().requireCurrent(
            List.of(item),
            Map.of(POSITION, visuallySameButNewer),
            OperationKind.ROLLBACK,
            null,
            true
        ));
    }

    @Test
    void validatesUndoAgainstTheOperationThatProducedTheLatestState() {
        UUID rolledBackOperation = UUID.fromString("c0000000-0000-0000-0000-00000000000c");
        OperationItem item = item("minecraft:gold_block");
        LatestHistoryValidator.LatestState ownRollback = new LatestHistoryValidator.LatestState(
            99L,
            BlockSnapshot.block("minecraft:gold_block"),
            rolledBackOperation
        );

        assertDoesNotThrow(() -> new LatestHistoryValidator().requireCurrent(
            List.of(item),
            Map.of(POSITION, ownRollback),
            OperationKind.UNDO,
            rolledBackOperation,
            true
        ));
        assertThrows(IllegalStateException.class, () -> new LatestHistoryValidator().requireCurrent(
            List.of(item),
            Map.of(POSITION, ownRollback),
            OperationKind.UNDO,
            UUID.randomUUID(),
            true
        ));
    }

    private static OperationItem item(String before) {
        return new OperationItem(
            0,
            POSITION,
            BlockSnapshot.block(before),
            BlockSnapshot.block("minecraft:stone"),
            List.of(1L)
        );
    }

    private static LatestHistoryValidator.LatestState latest(long id, String after) {
        return new LatestHistoryValidator.LatestState(id, BlockSnapshot.block(after), null);
    }
}
