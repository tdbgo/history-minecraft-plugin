package kr.playcity.history.storage;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

final class SourceIdCodec {
    private SourceIdCodec() {
    }

    static byte[] encode(List<Long> ids) {
        ByteArrayOutputStream output = new ByteArrayOutputStream(ids.size() * 3);
        long previous = 0L;
        for (long id : ids) {
            long delta = id - previous;
            writeUnsigned(output, (delta << 1) ^ (delta >> 63));
            previous = id;
        }
        return output.toByteArray();
    }

    static List<Long> decode(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return List.of();
        }
        List<Long> ids = new ArrayList<>();
        long previous = 0L;
        int offset = 0;
        while (offset < bytes.length) {
            long encoded = 0L;
            int shift = 0;
            while (true) {
                if (offset >= bytes.length || shift >= 64) {
                    throw new StorageException("Stored source ID sequence is malformed");
                }
                int value = bytes[offset++] & 0xFF;
                encoded |= (long) (value & 0x7F) << shift;
                if ((value & 0x80) == 0) {
                    break;
                }
                shift += 7;
            }
            long delta = (encoded >>> 1) ^ -(encoded & 1L);
            long id;
            try {
                id = Math.addExact(previous, delta);
            } catch (ArithmeticException exception) {
                throw new StorageException("Stored source ID sequence overflowed", exception);
            }
            ids.add(id);
            previous = id;
        }
        return List.copyOf(ids);
    }

    private static void writeUnsigned(ByteArrayOutputStream output, long value) {
        long remaining = value;
        while ((remaining & ~0x7FL) != 0L) {
            output.write((int) ((remaining & 0x7F) | 0x80));
            remaining >>>= 7;
        }
        output.write((int) remaining);
    }
}
