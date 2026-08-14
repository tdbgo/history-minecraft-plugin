package kr.playcity.history.storage;

import kr.playcity.history.model.BlockPosition;

import java.util.UUID;

final class PositionCodec {
    private PositionCodec() {
    }

    static long pack(BlockPosition position) {
        long encodedY = zigZag(position.y());
        return (encodedY << 8) | ((long) (position.z() & 15) << 4) | (position.x() & 15L);
    }

    static BlockPosition unpack(UUID worldId, int chunkX, int chunkZ, long packed) {
        int x = Math.addExact(Math.multiplyExact(chunkX, 16), (int) (packed & 15L));
        int z = Math.addExact(Math.multiplyExact(chunkZ, 16), (int) ((packed >>> 4) & 15L));
        int y = unZigZag(packed >>> 8);
        return new BlockPosition(worldId, x, y, z);
    }

    private static long zigZag(int value) {
        return ((long) value << 1) ^ (value >> 31);
    }

    private static int unZigZag(long value) {
        long decoded = (value >>> 1) ^ -(value & 1L);
        if (decoded < Integer.MIN_VALUE || decoded > Integer.MAX_VALUE) {
            throw new StorageException("Stored Y coordinate is outside the integer range");
        }
        return (int) decoded;
    }
}
