package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import kr.playcity.history.storage.OperationItemSource;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * Bounded binary spool used between a database streaming scan and rollback.
 * A completed footer makes truncated plans fail closed before world mutation.
 */
final class OperationPlanSpool {
    private static final int MAGIC = 0x48504C4E;
    private static final int VERSION = 1;
    private static final int ENTRY = 1;
    private static final int END = 0;
    private static final int MAXIMUM_STRING_BYTES = 1_048_576;
    private static final int MAXIMUM_PAYLOAD_BYTES = 32 * 1_024 * 1_024;
    private static final int MAXIMUM_SOURCE_IDS = 16_777_216;

    private OperationPlanSpool() {
    }

    static Path prepareDirectory(Path dataDirectory) {
        Objects.requireNonNull(dataDirectory, "dataDirectory");
        Path normalizedData = dataDirectory.toAbsolutePath().normalize();
        Path directory = normalizedData.resolve("rollback-plans").normalize();
        if (!directory.startsWith(normalizedData)) {
            throw new IllegalStateException("Rollback plan directory escaped the plugin data directory");
        }
        try {
            Files.createDirectories(directory);
            try (DirectoryStream<Path> stale = Files.newDirectoryStream(directory, "*.hplan")) {
                for (Path path : stale) {
                    Files.deleteIfExists(path);
                }
            }
            return directory;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to prepare the History rollback plan directory", exception);
        }
    }

    static Writer create(Path directory) {
        Objects.requireNonNull(directory, "directory");
        try {
            Path file = Files.createTempFile(directory, "history-", ".hplan");
            return new Writer(file);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to create a History rollback plan", exception);
        }
    }

    static Reader open(Path file) {
        return new Reader(file);
    }

    static void delete(Path file) {
        if (file == null) {
            return;
        }
        try {
            Files.deleteIfExists(file);
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to delete a History rollback plan", exception);
        }
    }

    static final class Writer implements AutoCloseable {
        private final Path path;
        private final DataOutputStream output;
        private long count;
        private boolean closed;

        private Writer(Path path) throws IOException {
            this.path = path.toAbsolutePath().normalize();
            this.output = new DataOutputStream(new BufferedOutputStream(Files.newOutputStream(
                this.path,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING
            )));
            output.writeInt(MAGIC);
            output.writeInt(VERSION);
        }

        Path path() {
            return path;
        }

        void write(OperationItem item) {
            Objects.requireNonNull(item, "item");
            requireOpen();
            try {
                output.writeByte(ENTRY);
                output.writeInt(item.sequence());
                BlockPosition position = item.position();
                output.writeLong(position.worldId().getMostSignificantBits());
                output.writeLong(position.worldId().getLeastSignificantBits());
                output.writeInt(position.x());
                output.writeInt(position.y());
                output.writeInt(position.z());
                writeSnapshot(output, item.before());
                writeSnapshot(output, item.after());
                output.writeInt(item.sourceIds().size());
                for (long sourceId : item.sourceIds()) {
                    output.writeLong(sourceId);
                }
                count++;
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to write a History rollback plan", exception);
            }
        }

        long count() {
            return count;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                output.writeByte(END);
                output.writeLong(count);
                output.close();
            } catch (IOException exception) {
                try {
                    output.close();
                } catch (IOException suppressed) {
                    exception.addSuppressed(suppressed);
                }
                throw new IllegalStateException("Unable to finish a History rollback plan", exception);
            }
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("Rollback plan writer is closed");
            }
        }
    }

    static final class Reader implements OperationItemSource {
        private final DataInputStream input;
        private long count;
        private boolean ended;
        private boolean closed;
        private OperationItem pending;

        private Reader(Path file) {
            Objects.requireNonNull(file, "file");
            try {
                input = new DataInputStream(new BufferedInputStream(Files.newInputStream(file)));
                int magic = input.readInt();
                int version = input.readInt();
                if (magic != MAGIC || version != VERSION) {
                    input.close();
                    throw new IllegalStateException("History rollback plan has an unsupported header");
                }
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to open a History rollback plan", exception);
            }
        }

        @Override
        public List<OperationItem> readBatch(int maximumItems) {
            if (maximumItems <= 0) {
                throw new IllegalArgumentException("maximumItems must be positive");
            }
            requireOpen();
            List<OperationItem> items = new ArrayList<>(Math.min(maximumItems, 8_192));
            while (items.size() < maximumItems) {
                OperationItem item = readOne();
                if (item == null) {
                    break;
                }
                items.add(item);
            }
            return List.copyOf(items);
        }

        PlanChunk readChunk() {
            requireOpen();
            OperationItem first = pending == null ? readOne() : takePending();
            if (first == null) {
                return null;
            }
            ExactChunkCoordinate coordinate = ExactChunkCoordinate.from(first.position());
            List<OperationItem> items = new ArrayList<>();
            items.add(first);
            while (true) {
                OperationItem next = readOne();
                if (next == null) {
                    break;
                }
                if (!ExactChunkCoordinate.from(next.position()).equals(coordinate)) {
                    pending = next;
                    break;
                }
                items.add(next);
            }
            return new PlanChunk(coordinate, items);
        }

        private OperationItem takePending() {
            OperationItem value = pending;
            pending = null;
            return value;
        }

        private OperationItem readOne() {
            if (ended) {
                return null;
            }
            try {
                int marker = input.readUnsignedByte();
                if (marker == END) {
                    long expected = input.readLong();
                    if (expected != count || input.read() != -1) {
                        throw new IllegalStateException("History rollback plan footer is inconsistent");
                    }
                    ended = true;
                    return null;
                }
                if (marker != ENTRY) {
                    throw new IllegalStateException("History rollback plan contains an invalid entry marker");
                }
                int sequence = input.readInt();
                UUID worldId = new UUID(input.readLong(), input.readLong());
                BlockPosition position = new BlockPosition(
                    worldId,
                    input.readInt(),
                    input.readInt(),
                    input.readInt()
                );
                BlockSnapshot before = readSnapshot(input);
                BlockSnapshot after = readSnapshot(input);
                int sourceCount = readBoundedLength(input, MAXIMUM_SOURCE_IDS, "source ID count");
                List<Long> sourceIds = new ArrayList<>(sourceCount);
                for (int index = 0; index < sourceCount; index++) {
                    sourceIds.add(input.readLong());
                }
                count++;
                return new OperationItem(sequence, position, before, after, sourceIds);
            } catch (EOFException exception) {
                throw new IllegalStateException("History rollback plan is truncated", exception);
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to read a History rollback plan", exception);
            }
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            try {
                input.close();
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to close a History rollback plan", exception);
            }
        }

        private void requireOpen() {
            if (closed) {
                throw new IllegalStateException("Rollback plan reader is closed");
            }
        }
    }

    record PlanChunk(ExactChunkCoordinate coordinate, List<OperationItem> items) {
        PlanChunk {
            coordinate = Objects.requireNonNull(coordinate, "coordinate");
            items = List.copyOf(items);
            if (items.isEmpty()) {
                throw new IllegalArgumentException("Plan chunk must not be empty");
            }
        }
    }

    private static void writeSnapshot(DataOutputStream output, BlockSnapshot snapshot) throws IOException {
        writeString(output, snapshot.blockData());
        writeString(output, snapshot.payloadType());
        byte[] payload = snapshot.payload();
        output.writeInt(payload.length);
        output.write(payload);
    }

    private static BlockSnapshot readSnapshot(DataInputStream input) throws IOException {
        String blockData = readString(input);
        String payloadType = readString(input);
        int payloadLength = readBoundedLength(input, MAXIMUM_PAYLOAD_BYTES, "snapshot payload");
        byte[] payload = input.readNBytes(payloadLength);
        if (payload.length != payloadLength) {
            throw new EOFException("Rollback plan snapshot payload is truncated");
        }
        return new BlockSnapshot(blockData, payloadType, payload);
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        if (bytes.length > MAXIMUM_STRING_BYTES) {
            throw new IllegalArgumentException("Rollback plan string is too large");
        }
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static String readString(DataInputStream input) throws IOException {
        int length = readBoundedLength(input, MAXIMUM_STRING_BYTES, "string");
        byte[] bytes = input.readNBytes(length);
        if (bytes.length != length) {
            throw new EOFException("Rollback plan string is truncated");
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int readBoundedLength(DataInputStream input, int maximum, String field) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > maximum) {
            throw new IllegalStateException("History rollback plan has an invalid " + field + " length");
        }
        return length;
    }
}
