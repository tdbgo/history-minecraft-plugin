package kr.playcity.history.storage;

import kr.playcity.history.model.BlockSnapshot;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StateCachePolicyTest {
    @Test
    void cachesOrdinaryAndSmallPayloadStatesButNotLargeContainerSnapshots() {
        assertTrue(StateCachePolicy.shouldCache(BlockSnapshot.block("minecraft:stone")));
        assertTrue(StateCachePolicy.shouldCache(new BlockSnapshot(
            "minecraft:chest", "inventory/v1", new byte[StateCachePolicy.MAXIMUM_CACHED_PAYLOAD_BYTES]
        )));
        assertFalse(StateCachePolicy.shouldCache(new BlockSnapshot(
            "minecraft:chest", "inventory/v1", new byte[StateCachePolicy.MAXIMUM_CACHED_PAYLOAD_BYTES + 1]
        )));
    }
}
