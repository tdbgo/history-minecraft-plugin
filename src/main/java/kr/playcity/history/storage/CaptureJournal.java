package kr.playcity.history.storage;

import kr.playcity.history.model.ActorKind;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.zip.CRC32C;

/**
 * Local append-only capture journal placed in front of the selected database.
 * All file access occurs on History-owned worker threads.
 */
final class CaptureJournal implements AutoCloseable {
    private static final byte[] MAGIC = new byte[] {'H', 'I', 'S', 'T', 'W', 'A', 'L', '1'};
    private static final int HEADER_BYTES = MAGIC.length;
    private static final int FRAME_HEADER_BYTES = Integer.BYTES * 2;
    private static final int CHECKPOINT_MAGIC = 0x48434b31;
    private static final int CHECKPOINT_BYTES = Integer.BYTES + Long.BYTES + Integer.BYTES;
    private static final int MAXIMUM_FRAME_BYTES = 64 * 1024 * 1024;
    private static final int MAXIMUM_STRING_BYTES = 4 * 1024 * 1024;
    private static final int MAXIMUM_PAYLOAD_BYTES = 48 * 1024 * 1024;
    private static final int TARGET_BATCH_BYTES = 16 * 1024 * 1024;

    private final Path journalFile;
    private final Path checkpointFile;
    private FileChannel channel;
    private long readOffset = HEADER_BYTES;
    private long replayUntil = HEADER_BYTES;
    private long idempotentRetryOffset = -1L;
    private JournalBatch inFlight;
    private boolean resetPending;
    private volatile long pendingCount;
    private volatile long backlogBytes;
    private volatile boolean truncatedTail;

    CaptureJournal(Path journalFile) {
        this.journalFile = journalFile.toAbsolutePath().normalize();
        this.checkpointFile = this.journalFile.resolveSibling(this.journalFile.getFileName() + ".checkpoint");
    }

    synchronized void open() {
        if (channel != null) {
            return;
        }
        try {
            Path parent = journalFile.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            channel = FileChannel.open(
                journalFile,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
            );
            initializeHeader();
            scanAndRecover();
        } catch (IOException | RuntimeException failure) {
            closeQuietly();
            throw failure instanceof StorageException storage
                ? storage
                : new StorageException("Unable to open the History capture journal", failure);
        }
    }

    synchronized void append(List<ChangeRecord> changes) {
        if (changes.isEmpty()) {
            return;
        }
        requireOpen();
        if (resetPending) {
            throw new StorageException("History capture journal checkpoint reset must finish before appending");
        }
        long originalLength = -1L;
        try {
            originalLength = channel.size();
            long writeOffset = originalLength;
            ByteBuffer output = ByteBuffer.allocate(256 * 1024);
            for (ChangeRecord change : changes) {
                if (change.captureId() == null) {
                    throw new StorageException("A journaled History change requires a capture identity");
                }
                byte[] payload = encode(change);
                CRC32C crc = new CRC32C();
                crc.update(payload, 0, payload.length);
                if (output.remaining() < FRAME_HEADER_BYTES) {
                    writeOffset = flushBuffer(output, writeOffset);
                }
                output.putInt(payload.length).putInt((int) crc.getValue());
                int cursor = 0;
                while (cursor < payload.length) {
                    if (!output.hasRemaining()) {
                        writeOffset = flushBuffer(output, writeOffset);
                    }
                    int amount = Math.min(output.remaining(), payload.length - cursor);
                    output.put(payload, cursor, amount);
                    cursor += amount;
                }
            }
            writeOffset = flushBuffer(output, writeOffset);
            channel.force(true);
            pendingCount = Math.addExact(pendingCount, changes.size());
            backlogBytes = Math.max(0L, writeOffset - readOffset);
        } catch (IOException | RuntimeException failure) {
            if (originalLength >= HEADER_BYTES) {
                try {
                    channel.truncate(originalLength);
                    channel.force(false);
                } catch (IOException truncateFailure) {
                    failure.addSuppressed(truncateFailure);
                }
            }
            throw failure instanceof StorageException storage
                ? storage
                : new StorageException("Unable to append to the History capture journal", failure);
        }
    }

    synchronized JournalBatch readBatch(int maximumRecords) {
        if (maximumRecords <= 0) {
            throw new IllegalArgumentException("maximumRecords must be positive");
        }
        requireOpen();
        try {
            if (inFlight != null) {
                return inFlight;
            }
            long length = channel.size();
            if (readOffset >= length) {
                return JournalBatch.empty(readOffset);
            }
            long cursor = readOffset;
            boolean recovered = readOffset < replayUntil;
            boolean replaying = recovered || readOffset == idempotentRetryOffset;
            List<ChangeRecord> changes = new ArrayList<>(Math.min(maximumRecords, 8_192));
            while (changes.size() < maximumRecords && cursor < length) {
                Frame frame = readFrame(cursor, length, false);
                if (!changes.isEmpty() && frame.endOffset() - readOffset > TARGET_BATCH_BYTES) {
                    break;
                }
                changes.add(decode(frame.payload()));
                cursor = frame.endOffset();
                if ((recovered && cursor >= replayUntil) || cursor - readOffset >= TARGET_BATCH_BYTES) {
                    break;
                }
            }
            inFlight = new JournalBatch(
                List.copyOf(changes),
                readOffset,
                cursor,
                replaying,
                recovered
            );
            return inFlight;
        } catch (IOException failure) {
            throw new StorageException("Unable to read the History capture journal", failure);
        }
    }

    synchronized void acknowledge(JournalBatch batch) {
        requireOpen();
        if (batch.changes().isEmpty()) {
            return;
        }
        if (batch != inFlight || batch.startOffset() != readOffset || batch.endOffset() <= batch.startOffset()) {
            throw new StorageException("History capture journal acknowledgement is out of order");
        }
        try {
            long length = channel.size();
            long remaining = Math.max(0L, pendingCount - batch.changes().size());
            if (resetPending || batch.endOffset() == length) {
                // Reset the checkpoint durably before reusing offsets. A crash between
                // these steps can replay old UUIDs, but can never skip new captures.
                resetPending = true;
                writeCheckpoint(HEADER_BYTES);
                channel.truncate(HEADER_BYTES);
                channel.force(true);
                readOffset = HEADER_BYTES;
                replayUntil = HEADER_BYTES;
                backlogBytes = 0L;
                pendingCount = remaining;
                resetPending = false;
                if (idempotentRetryOffset == batch.startOffset()) {
                    idempotentRetryOffset = -1L;
                }
            } else {
                writeCheckpoint(batch.endOffset());
                readOffset = batch.endOffset();
                backlogBytes = Math.max(0L, length - readOffset);
                pendingCount = remaining;
                if (idempotentRetryOffset == batch.startOffset()) {
                    idempotentRetryOffset = -1L;
                }
            }
            inFlight = null;
        } catch (IOException failure) {
            throw new StorageException("Unable to checkpoint the History capture journal", failure);
        }
    }

    synchronized void requireIdempotentRetry(JournalBatch batch) {
        requireOpen();
        if (batch != inFlight || batch.startOffset() != readOffset) {
            throw new StorageException("History capture journal retry marker is out of order");
        }
        idempotentRetryOffset = readOffset;
        inFlight = new JournalBatch(batch.changes(), batch.startOffset(), batch.endOffset(), true, batch.recovered());
    }

    long pendingCount() {
        return pendingCount;
    }

    long backlogBytes() {
        return backlogBytes;
    }

    boolean truncatedTail() {
        return truncatedTail;
    }

    synchronized void verifyWritable() {
        requireOpen();
        try {
            channel.force(true);
            writeCheckpoint(readOffset);
        } catch (IOException failure) {
            throw new StorageException("History capture journal is not writable", failure);
        }
    }

    @Override
    public synchronized void close() {
        if (channel == null) {
            return;
        }
        try {
            channel.force(false);
            channel.close();
        } catch (IOException failure) {
            throw new StorageException("Unable to close the History capture journal", failure);
        } finally {
            channel = null;
        }
    }

    private void initializeHeader() throws IOException {
        long length = channel.size();
        if (length == 0L) {
            writeFully(ByteBuffer.wrap(MAGIC), 0L);
            channel.force(false);
            return;
        }
        if (length < HEADER_BYTES) {
            throw new StorageException("History capture journal header is truncated");
        }
        ByteBuffer header = ByteBuffer.allocate(HEADER_BYTES);
        readFully(header, 0L);
        if (!java.util.Arrays.equals(header.array(), MAGIC)) {
            throw new StorageException("History capture journal has an unsupported format");
        }
    }

    private void scanAndRecover() throws IOException {
        inFlight = null;
        resetPending = false;
        idempotentRetryOffset = -1L;
        truncatedTail = false;
        long length = channel.size();
        long requestedCheckpoint = readCheckpoint();
        long cursor = HEADER_BYTES;
        long recordsAfterCheckpoint = 0L;
        boolean checkpointBoundary = requestedCheckpoint == HEADER_BYTES;
        while (cursor < length) {
            Frame frame;
            try {
                frame = readFrame(cursor, length, true);
            } catch (IncompleteTailException incomplete) {
                channel.truncate(cursor);
                channel.force(false);
                length = cursor;
                truncatedTail = true;
                break;
            }
            cursor = frame.endOffset();
            if (cursor == requestedCheckpoint) {
                checkpointBoundary = true;
            }
            if (cursor > requestedCheckpoint) {
                recordsAfterCheckpoint++;
            }
        }
        if (!checkpointBoundary || requestedCheckpoint < HEADER_BYTES || requestedCheckpoint > length) {
            requestedCheckpoint = HEADER_BYTES;
            recordsAfterCheckpoint = countFrames(HEADER_BYTES, length);
            writeCheckpoint(requestedCheckpoint);
        }
        readOffset = requestedCheckpoint;
        replayUntil = length;
        pendingCount = recordsAfterCheckpoint;
        backlogBytes = Math.max(0L, length - readOffset);
    }

    private long countFrames(long start, long length) throws IOException {
        long count = 0L;
        long cursor = start;
        while (cursor < length) {
            Frame frame = readFrame(cursor, length, false);
            cursor = frame.endOffset();
            count++;
        }
        return count;
    }

    private Frame readFrame(long offset, long fileLength, boolean recoverTail) throws IOException {
        if (fileLength - offset < FRAME_HEADER_BYTES) {
            if (recoverTail) {
                throw new IncompleteTailException();
            }
            throw new EOFException("History capture journal frame header is truncated");
        }
        ByteBuffer header = ByteBuffer.allocate(FRAME_HEADER_BYTES);
        readFully(header, offset);
        header.flip();
        int length = header.getInt();
        int expectedCrc = header.getInt();
        if (length <= 0 || length > MAXIMUM_FRAME_BYTES) {
            throw new StorageException("History capture journal frame length is invalid: " + length);
        }
        long end = offset + FRAME_HEADER_BYTES + length;
        if (end > fileLength) {
            if (recoverTail) {
                throw new IncompleteTailException();
            }
            throw new EOFException("History capture journal frame payload is truncated");
        }
        byte[] payload = new byte[length];
        ByteBuffer body = ByteBuffer.wrap(payload);
        readFully(body, offset + FRAME_HEADER_BYTES);
        CRC32C crc = new CRC32C();
        crc.update(payload, 0, payload.length);
        if ((int) crc.getValue() != expectedCrc) {
            throw new StorageException("History capture journal frame checksum is invalid at offset " + offset);
        }
        return new Frame(payload, end);
    }

    private long readCheckpoint() throws IOException {
        if (!Files.isRegularFile(checkpointFile)) {
            return HEADER_BYTES;
        }
        byte[] bytes = Files.readAllBytes(checkpointFile);
        if (bytes.length != CHECKPOINT_BYTES) {
            return HEADER_BYTES;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.getInt() != CHECKPOINT_MAGIC) {
            return HEADER_BYTES;
        }
        long offset = buffer.getLong();
        int expectedCrc = buffer.getInt();
        CRC32C crc = new CRC32C();
        ByteBuffer encoded = ByteBuffer.allocate(Long.BYTES).putLong(offset);
        crc.update(encoded.array(), 0, Long.BYTES);
        return (int) crc.getValue() == expectedCrc ? offset : HEADER_BYTES;
    }

    private void writeCheckpoint(long offset) throws IOException {
        CRC32C crc = new CRC32C();
        ByteBuffer encodedOffset = ByteBuffer.allocate(Long.BYTES).putLong(offset);
        crc.update(encodedOffset.array(), 0, Long.BYTES);
        ByteBuffer checkpoint = ByteBuffer.allocate(CHECKPOINT_BYTES)
            .putInt(CHECKPOINT_MAGIC)
            .putLong(offset)
            .putInt((int) crc.getValue())
            .flip();
        Path temporary = checkpointFile.resolveSibling(checkpointFile.getFileName() + ".tmp");
        try (FileChannel output = FileChannel.open(
            temporary,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING,
            StandardOpenOption.WRITE
        )) {
            while (checkpoint.hasRemaining()) {
                output.write(checkpoint);
            }
            output.force(true);
        }
        try {
            Files.move(
                temporary,
                checkpointFile,
                StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING
            );
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, checkpointFile, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private static byte[] encode(ChangeRecord change) {
        try {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(512);
            try (DataOutputStream output = new DataOutputStream(bytes)) {
                writeUuid(output, change.captureId());
                output.writeLong(change.occurredAt());
                writeUuid(output, change.position().worldId());
                output.writeInt(change.position().x());
                output.writeInt(change.position().y());
                output.writeInt(change.position().z());
                writeNullableUuid(output, change.actor().uuid());
                writeString(output, change.actor().name());
                output.writeInt(change.actor().kind().storageCode());
                output.writeInt(change.cause().storageCode());
                writeSnapshot(output, change.before());
                writeSnapshot(output, change.after());
                writeNullableUuid(output, change.operationId());
                writeNullableUuid(output, change.batchId());
                writeString(output, change.metadata());
            }
            byte[] result = bytes.toByteArray();
            if (result.length > MAXIMUM_FRAME_BYTES) {
                throw new StorageException("History capture journal frame exceeds the supported size");
            }
            return result;
        } catch (IOException impossible) {
            throw new StorageException("Unable to encode a History capture journal frame", impossible);
        }
    }

    private static ChangeRecord decode(byte[] payload) {
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            UUID captureId = readUuid(input);
            long occurredAt = input.readLong();
            BlockPosition position = new BlockPosition(
                readUuid(input), input.readInt(), input.readInt(), input.readInt()
            );
            ActorRef actor = new ActorRef(
                readNullableUuid(input),
                readString(input),
                ActorKind.fromStorageCode(input.readInt())
            );
            ChangeCause cause = ChangeCause.fromStorageCode(input.readInt());
            BlockSnapshot before = readSnapshot(input);
            BlockSnapshot after = readSnapshot(input);
            UUID operationId = readNullableUuid(input);
            UUID batchId = readNullableUuid(input);
            String metadata = readString(input);
            if (input.read() != -1) {
                throw new StorageException("History capture journal frame contains trailing data");
            }
            return new ChangeRecord(
                0L, occurredAt, position, actor, cause, before, after,
                operationId, batchId, metadata, captureId
            );
        } catch (IOException | IllegalArgumentException failure) {
            throw new StorageException("Unable to decode a History capture journal frame", failure);
        }
    }

    private static void writeSnapshot(DataOutputStream output, BlockSnapshot snapshot) throws IOException {
        writeString(output, snapshot.blockData());
        writeString(output, snapshot.payloadType());
        byte[] payload = snapshot.payload();
        if (payload.length > MAXIMUM_PAYLOAD_BYTES) {
            throw new StorageException("History block payload exceeds the capture journal limit");
        }
        output.writeInt(payload.length);
        output.write(payload);
    }

    private static BlockSnapshot readSnapshot(DataInputStream input) throws IOException {
        String blockData = readString(input);
        String payloadType = readString(input);
        int length = input.readInt();
        if (length < 0 || length > MAXIMUM_PAYLOAD_BYTES) {
            throw new StorageException("History capture journal block payload length is invalid");
        }
        byte[] payload = input.readNBytes(length);
        if (payload.length != length) {
            throw new EOFException("History capture journal block payload is truncated");
        }
        return new BlockSnapshot(blockData, payloadType, payload);
    }

    private static void writeString(DataOutputStream output, String value) throws IOException {
        byte[] encoded = value.getBytes(StandardCharsets.UTF_8);
        if (encoded.length > MAXIMUM_STRING_BYTES) {
            throw new StorageException("History capture journal string exceeds the supported size");
        }
        output.writeInt(encoded.length);
        output.write(encoded);
    }

    private static String readString(DataInputStream input) throws IOException {
        int length = input.readInt();
        if (length < 0 || length > MAXIMUM_STRING_BYTES) {
            throw new StorageException("History capture journal string length is invalid");
        }
        byte[] encoded = input.readNBytes(length);
        if (encoded.length != length) {
            throw new EOFException("History capture journal string is truncated");
        }
        return new String(encoded, StandardCharsets.UTF_8);
    }

    private static void writeUuid(DataOutputStream output, UUID value) throws IOException {
        output.writeLong(value.getMostSignificantBits());
        output.writeLong(value.getLeastSignificantBits());
    }

    private static UUID readUuid(DataInputStream input) throws IOException {
        return new UUID(input.readLong(), input.readLong());
    }

    private static void writeNullableUuid(DataOutputStream output, UUID value) throws IOException {
        output.writeBoolean(value != null);
        if (value != null) {
            writeUuid(output, value);
        }
    }

    private static UUID readNullableUuid(DataInputStream input) throws IOException {
        return input.readBoolean() ? readUuid(input) : null;
    }

    private void writeFully(ByteBuffer source, long offset) throws IOException {
        long cursor = offset;
        while (source.hasRemaining()) {
            int written = channel.write(source, cursor);
            if (written <= 0) {
                throw new IOException("History capture journal write made no progress");
            }
            cursor += written;
        }
    }

    private long flushBuffer(ByteBuffer buffer, long offset) throws IOException {
        int length = buffer.position();
        buffer.flip();
        writeFully(buffer, offset);
        buffer.clear();
        return offset + length;
    }

    private void readFully(ByteBuffer target, long offset) throws IOException {
        long cursor = offset;
        while (target.hasRemaining()) {
            int read = channel.read(target, cursor);
            if (read < 0) {
                throw new EOFException("History capture journal ended unexpectedly");
            }
            if (read == 0) {
                throw new IOException("History capture journal read made no progress");
            }
            cursor += read;
        }
    }

    private void requireOpen() {
        if (channel == null) {
            throw new StorageException("History capture journal is not open");
        }
    }

    private void closeQuietly() {
        if (channel != null) {
            try {
                channel.close();
            } catch (IOException ignored) {
                // Preserve the opening failure.
            }
            channel = null;
        }
    }

    record JournalBatch(
        List<ChangeRecord> changes,
        long startOffset,
        long endOffset,
        boolean replayed,
        boolean recovered
    ) {
        private static JournalBatch empty(long offset) {
            return new JournalBatch(List.of(), offset, offset, false, false);
        }
    }

    private record Frame(byte[] payload, long endOffset) {
    }

    private static final class IncompleteTailException extends IOException {
        private static final long serialVersionUID = 1L;
    }
}
