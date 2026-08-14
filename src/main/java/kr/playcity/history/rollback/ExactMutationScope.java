package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Immutable, exact-coordinate capability for a rollback execution.
 *
 * <p>The scope deliberately has no cuboid or chunk-range mutation API. A caller
 * can mutate only an operation item that was present when this capability was
 * created. This keeps rollback independent from WorldEdit/FAWE restore paths
 * and prevents a range calculation from expanding a write outside the preview.</p>
 */
public final class ExactMutationScope {
    private static final Comparator<OperationItem> STABLE_ORDER = Comparator
        .comparingInt(OperationItem::sequence)
        .thenComparing(item -> item.position().worldId())
        .thenComparingInt(item -> item.position().x())
        .thenComparingInt(item -> item.position().y())
        .thenComparingInt(item -> item.position().z());

    private final Map<BlockPosition, OperationItem> allowed;
    private final Map<ExactChunkCoordinate, List<OperationItem>> itemsByChunk;
    private final List<ExactChunkCoordinate> chunks;
    private final String fingerprint;

    private ExactMutationScope(
        Map<BlockPosition, OperationItem> allowed,
        Map<ExactChunkCoordinate, List<OperationItem>> itemsByChunk,
        List<ExactChunkCoordinate> chunks,
        String fingerprint
    ) {
        this.allowed = Map.copyOf(allowed);
        this.itemsByChunk = Map.copyOf(itemsByChunk);
        this.chunks = List.copyOf(chunks);
        this.fingerprint = fingerprint;
    }

    public static ExactMutationScope create(
        List<OperationItem> items,
        int maximumBlocks,
        int maximumChunks
    ) {
        Objects.requireNonNull(items, "items");
        if (maximumBlocks <= 0 || maximumChunks <= 0) {
            throw new IllegalArgumentException("Mutation scope limits must be positive");
        }
        if (items.isEmpty()) {
            throw new IllegalArgumentException("Mutation scope must contain at least one block");
        }
        if (items.size() > maximumBlocks) {
            throw new IllegalArgumentException("Mutation scope exceeds the block limit");
        }

        Map<BlockPosition, OperationItem> allowed = new HashMap<>();
        Set<Integer> sequences = new HashSet<>();
        Set<ExactChunkCoordinate> chunks = new HashSet<>();
        Set<UUID> worlds = new HashSet<>();
        for (OperationItem item : items) {
            Objects.requireNonNull(item, "operation item");
            if (!sequences.add(item.sequence())) {
                throw new IllegalArgumentException("Mutation scope contains a duplicate sequence");
            }
            if (allowed.putIfAbsent(item.position(), item) != null) {
                throw new IllegalArgumentException("Mutation scope contains a duplicate block position");
            }
            BlockPosition position = item.position();
            worlds.add(position.worldId());
            chunks.add(ExactChunkCoordinate.from(position));
        }
        if (worlds.size() != 1) {
            throw new IllegalArgumentException("One rollback operation must remain inside one world");
        }
        if (chunks.size() > maximumChunks) {
            throw new IllegalArgumentException("Mutation scope exceeds the chunk limit");
        }

        List<OperationItem> stable = new ArrayList<>(items);
        stable.sort(STABLE_ORDER);
        Map<ExactChunkCoordinate, List<OperationItem>> mutableByChunk = new HashMap<>();
        for (OperationItem item : stable) {
            mutableByChunk.computeIfAbsent(ExactChunkCoordinate.from(item.position()), ignored -> new ArrayList<>())
                .add(item);
        }
        Map<ExactChunkCoordinate, List<OperationItem>> itemsByChunk = new HashMap<>();
        mutableByChunk.forEach((chunk, chunkItems) -> itemsByChunk.put(chunk, List.copyOf(chunkItems)));
        List<ExactChunkCoordinate> stableChunks = new ArrayList<>(chunks);
        stableChunks.sort(ExactChunkCoordinate.STABLE_ORDER);
        return new ExactMutationScope(allowed, itemsByChunk, stableChunks, fingerprint(stable));
    }

    public void requireAllowed(OperationItem item) {
        Objects.requireNonNull(item, "item");
        OperationItem expected = allowed.get(item.position());
        if (!item.equals(expected)) {
            throw new IllegalStateException("Rollback attempted a mutation outside its exact preview scope");
        }
    }

    public boolean contains(BlockPosition position) {
        return allowed.containsKey(position);
    }

    public void requireChunkAllowed(ExactChunkCoordinate chunk) {
        Objects.requireNonNull(chunk, "chunk");
        if (!itemsByChunk.containsKey(chunk)) {
            throw new IllegalStateException("Rollback attempted to load a chunk outside its exact preview scope");
        }
    }

    public boolean containsChunk(ExactChunkCoordinate chunk) {
        return itemsByChunk.containsKey(chunk);
    }

    public List<ExactChunkCoordinate> chunks() {
        return chunks;
    }

    public List<OperationItem> itemsIn(ExactChunkCoordinate chunk) {
        requireChunkAllowed(chunk);
        return itemsByChunk.get(chunk);
    }

    public int blockCount() {
        return allowed.size();
    }

    public int chunkCount() {
        return chunks.size();
    }

    public String fingerprint() {
        return fingerprint;
    }

    private static String fingerprint(List<OperationItem> items) {
        MessageDigest digest;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
        updateInt(digest, items.size());
        for (OperationItem item : items) {
            updateInt(digest, item.sequence());
            BlockPosition position = item.position();
            updateLong(digest, position.worldId().getMostSignificantBits());
            updateLong(digest, position.worldId().getLeastSignificantBits());
            updateInt(digest, position.x());
            updateInt(digest, position.y());
            updateInt(digest, position.z());
            updateSnapshot(digest, item.before());
            updateSnapshot(digest, item.after());
            updateInt(digest, item.sourceIds().size());
            item.sourceIds().forEach(id -> updateLong(digest, id));
        }
        return HexFormat.of().formatHex(digest.digest());
    }

    private static void updateSnapshot(MessageDigest digest, BlockSnapshot snapshot) {
        updateBytes(digest, snapshot.blockData().getBytes(StandardCharsets.UTF_8));
        updateBytes(digest, snapshot.payloadType().getBytes(StandardCharsets.UTF_8));
        updateBytes(digest, snapshot.payload());
    }

    private static void updateBytes(MessageDigest digest, byte[] bytes) {
        updateInt(digest, bytes.length);
        digest.update(bytes);
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
    }

    private static void updateLong(MessageDigest digest, long value) {
        digest.update(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
    }
}
