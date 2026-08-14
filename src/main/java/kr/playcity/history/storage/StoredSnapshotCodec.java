package kr.playcity.history.storage;

import kr.playcity.history.model.BlockSnapshot;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

final class StoredSnapshotCodec {
    private static final String DEFLATE_PREFIX = "deflate/";
    private static final int COMPRESSION_THRESHOLD = 256;
    private static final int MINIMUM_SAVING = 32;
    private static final int MAXIMUM_DECOMPRESSED_SIZE = 32 * 1_024 * 1_024;

    private StoredSnapshotCodec() {
    }

    static Encoded encode(BlockSnapshot snapshot) {
        byte[] payload = snapshot.payload();
        if (payload.length < COMPRESSION_THRESHOLD || snapshot.payloadType().isEmpty()) {
            return new Encoded(snapshot.payloadType(), payload);
        }
        byte[] compressed = deflate(payload);
        if (compressed.length + MINIMUM_SAVING >= payload.length) {
            return new Encoded(snapshot.payloadType(), payload);
        }
        return new Encoded(DEFLATE_PREFIX + snapshot.payloadType(), compressed);
    }

    static BlockSnapshot decode(String blockData, String payloadType, byte[] payload) {
        if (!payloadType.startsWith(DEFLATE_PREFIX)) {
            return new BlockSnapshot(blockData, payloadType, payload);
        }
        String decodedType = payloadType.substring(DEFLATE_PREFIX.length());
        if (decodedType.isEmpty()) {
            throw new StorageException("Stored block-state payload has an invalid compression marker");
        }
        return new BlockSnapshot(blockData, decodedType, inflate(payload));
    }

    private static byte[] deflate(byte[] input) {
        Deflater deflater = new Deflater(Deflater.DEFAULT_COMPRESSION);
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream(input.length / 2);
             DeflaterOutputStream output = new DeflaterOutputStream(bytes, deflater)) {
            output.write(input);
            output.finish();
            return bytes.toByteArray();
        } catch (IOException exception) {
            throw new StorageException("Unable to compress a block-state payload", exception);
        } finally {
            deflater.end();
        }
    }

    private static byte[] inflate(byte[] input) {
        Inflater inflater = new Inflater();
        try (ByteArrayInputStream bytes = new ByteArrayInputStream(input);
             InflaterInputStream stream = new InflaterInputStream(bytes, inflater);
             ByteArrayOutputStream output = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8_192];
            int total = 0;
            int read;
            while ((read = stream.read(buffer)) != -1) {
                total += read;
                if (total > MAXIMUM_DECOMPRESSED_SIZE) {
                    throw new StorageException("Stored block-state payload exceeds the safety limit");
                }
                output.write(buffer, 0, read);
            }
            return output.toByteArray();
        } catch (IOException exception) {
            throw new StorageException("Unable to decompress a block-state payload", exception);
        } finally {
            inflater.end();
        }
    }

    record Encoded(String payloadType, byte[] payload) {
        Encoded {
            payload = payload.clone();
        }

        @Override
        public byte[] payload() {
            return payload.clone();
        }
    }
}
