package kr.playcity.history.storage;

import kr.playcity.history.model.BlockPosition;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StorageCodecTest {
    @Test
    void roundTripsCoordinatesAcrossNegativeChunksAndWorldHeights() {
        UUID worldId = UUID.randomUUID();
        for (BlockPosition position : List.of(
            new BlockPosition(worldId, 0, 0, 0),
            new BlockPosition(worldId, -1, -64, -1),
            new BlockPosition(worldId, -30_000_000, 319, 30_000_000),
            new BlockPosition(worldId, Integer.MIN_VALUE, Integer.MIN_VALUE, Integer.MIN_VALUE),
            new BlockPosition(worldId, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE)
        )) {
            assertEquals(
                position,
                PositionCodec.unpack(worldId, position.chunkX(), position.chunkZ(), PositionCodec.pack(position))
            );
        }
    }

    @Test
    void deltaVarintsCompressNearbySourceIdsAndRoundTripDescendingIds() {
        List<Long> ids = List.of(9_001L, 9_000L, 8_998L, 8_997L, 8_000L);
        byte[] encoded = SourceIdCodec.encode(ids);

        assertEquals(ids, SourceIdCodec.decode(encoded));
        assertTrue(encoded.length < ids.size() * Long.BYTES);
    }

    @Test
    void storesUuidsInFixedSixteenByteBinaryForm() {
        UUID id = UUID.randomUUID();
        byte[] encoded = UuidCodec.encode(id);

        assertEquals(16, encoded.length);
        assertEquals(id, UuidCodec.decode(encoded));
        assertArrayEquals(encoded, UuidCodec.encode(UuidCodec.decode(encoded)));
    }
}
