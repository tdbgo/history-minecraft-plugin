package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExactMutationScopeTest {
    private static final UUID WORLD = UUID.fromString("40000000-0000-0000-0000-000000000004");

    @Test
    void permitsOnlyTheExactPreviewedMutation() {
        OperationItem allowed = item(0, position(1, 64, 1), "minecraft:stone", "minecraft:air");
        ExactMutationScope scope = ExactMutationScope.create(List.of(allowed), 100, 10);

        scope.requireAllowed(allowed);
        assertTrue(scope.contains(allowed.position()));
        assertFalse(scope.contains(position(2, 64, 1)));
        assertEquals(1, scope.blockCount());
        assertEquals(1, scope.chunkCount());
        assertEquals(64, scope.fingerprint().length());

        OperationItem alteredTarget = item(
            0,
            allowed.position(),
            "minecraft:stone",
            "minecraft:diamond_block"
        );
        assertThrows(IllegalStateException.class, () -> scope.requireAllowed(alteredTarget));
        assertThrows(IllegalStateException.class, () -> scope.requireAllowed(
            item(1, position(2, 64, 1), "minecraft:stone", "minecraft:air")
        ));
    }

    @Test
    void rejectsDuplicateCoordinatesAndSequences() {
        OperationItem first = item(0, position(1, 64, 1), "minecraft:stone", "minecraft:air");
        OperationItem duplicatePosition = item(
            1,
            first.position(),
            "minecraft:dirt",
            "minecraft:air"
        );
        OperationItem duplicateSequence = item(
            0,
            position(2, 64, 1),
            "minecraft:dirt",
            "minecraft:air"
        );

        assertThrows(IllegalArgumentException.class, () ->
            ExactMutationScope.create(List.of(first, duplicatePosition), 100, 10));
        assertThrows(IllegalArgumentException.class, () ->
            ExactMutationScope.create(List.of(first, duplicateSequence), 100, 10));
    }

    @Test
    void rejectsCrossWorldAndOverwideChunkScopes() {
        OperationItem origin = item(0, position(1, 64, 1), "minecraft:stone", "minecraft:air");
        OperationItem anotherChunk = item(
            1,
            position(32, 64, 1),
            "minecraft:stone",
            "minecraft:air"
        );
        OperationItem anotherWorld = item(
            1,
            new BlockPosition(UUID.randomUUID(), 2, 64, 1),
            "minecraft:stone",
            "minecraft:air"
        );

        assertThrows(IllegalArgumentException.class, () ->
            ExactMutationScope.create(List.of(origin, anotherChunk), 100, 1));
        assertThrows(IllegalArgumentException.class, () ->
            ExactMutationScope.create(List.of(origin, anotherWorld), 100, 10));
    }

    @Test
    void fingerprintIsStableWhenTheContainerListIsReordered() {
        OperationItem first = item(0, position(-1, 64, 1), "minecraft:stone", "minecraft:bricks");
        OperationItem second = item(1, position(17, 80, -17), "minecraft:dirt", "minecraft:grass_block");

        ExactMutationScope forward = ExactMutationScope.create(List.of(first, second), 100, 10);
        ExactMutationScope reversed = ExactMutationScope.create(List.of(second, first), 100, 10);

        assertEquals(forward.fingerprint(), reversed.fingerprint());
    }

    @Test
    void exposesOnlyExactDistantChunksWithoutFillingTheGap() {
        OperationItem near = item(0, position(1, 64, 1), "minecraft:stone", "minecraft:air");
        OperationItem far = item(
            1,
            position(1_000_000, 70, -1_000_000),
            "minecraft:quartz_block",
            "minecraft:air"
        );

        ExactMutationScope scope = ExactMutationScope.create(List.of(far, near), 100, 2);
        ExactChunkCoordinate nearChunk = ExactChunkCoordinate.from(near.position());
        ExactChunkCoordinate farChunk = ExactChunkCoordinate.from(far.position());

        assertEquals(List.of(nearChunk, farChunk), scope.chunks());
        assertEquals(List.of(near), scope.itemsIn(nearChunk));
        assertEquals(List.of(far), scope.itemsIn(farChunk));
        assertTrue(scope.containsChunk(farChunk));
        assertFalse(scope.containsChunk(new ExactChunkCoordinate(WORLD, 1, 0)));
        assertThrows(IllegalStateException.class, () ->
            scope.requireChunkAllowed(new ExactChunkCoordinate(WORLD, 1, 0)));
    }

    private static OperationItem item(
        int sequence,
        BlockPosition position,
        String before,
        String after
    ) {
        return new OperationItem(
            sequence,
            position,
            BlockSnapshot.block(before),
            BlockSnapshot.block(after),
            List.of((long) sequence + 1L)
        );
    }

    private static BlockPosition position(int x, int y, int z) {
        return new BlockPosition(WORLD, x, y, z);
    }
}
