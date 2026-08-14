package kr.playcity.history.rollback;

import org.bukkit.Bukkit;
import org.bukkit.Chunk;
import org.bukkit.World;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;

/**
 * Loads only exact chunks authorized by an {@link ExactMutationScope}.
 *
 * <p>Leases are exclusive per chunk, globally concurrency limited, and hold a
 * plugin chunk ticket only for their useful lifetime. The manager deliberately
 * exposes no range-loading operation.</p>
 */
final class ScopedChunkLeaseManager {
    private final Runtime runtime;
    private final int maximumConcurrentLeases;
    private final int loadTimeoutSeconds;
    private final boolean generateMissingChunks;
    private final Map<ExactChunkCoordinate, Entry> entries = new HashMap<>();
    private final ArrayDeque<Entry> loadQueue = new ArrayDeque<>();
    private int activeEntries;
    private boolean closing;

    ScopedChunkLeaseManager(
        JavaPlugin plugin,
        int maximumConcurrentLeases,
        int loadTimeoutSeconds,
        boolean generateMissingChunks
    ) {
        this(
            new PaperRuntime(Objects.requireNonNull(plugin, "plugin")),
            maximumConcurrentLeases,
            loadTimeoutSeconds,
            generateMissingChunks
        );
    }

    ScopedChunkLeaseManager(
        Runtime runtime,
        int maximumConcurrentLeases,
        int loadTimeoutSeconds,
        boolean generateMissingChunks
    ) {
        this.runtime = Objects.requireNonNull(runtime, "runtime");
        if (maximumConcurrentLeases <= 0 || loadTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("Chunk loading limits must be positive");
        }
        this.maximumConcurrentLeases = maximumConcurrentLeases;
        this.loadTimeoutSeconds = loadTimeoutSeconds;
        this.generateMissingChunks = generateMissingChunks;
    }

    CompletableFuture<Lease> acquire(ExactMutationScope scope, ExactChunkCoordinate target) {
        Objects.requireNonNull(scope, "scope");
        Objects.requireNonNull(target, "target");
        scope.requireChunkAllowed(target);

        CompletableFuture<Lease> result = new CompletableFuture<>();
        runOnServerThread(() -> enqueue(scope, target, result), result);
        return result;
    }

    void close() {
        requireServerThread();
        if (closing) {
            return;
        }
        closing = true;
        IllegalStateException failure = new IllegalStateException("History chunk loader is closing");
        for (Entry entry : entries.values()) {
            while (!entry.waiters.isEmpty()) {
                entry.waiters.removeFirst().completeExceptionally(failure);
            }
            removeOwnedTicket(entry);
            releasePermit(entry);
            entry.activeLeaseId = 0L;
        }
        entries.clear();
        loadQueue.clear();
    }

    private void enqueue(
        ExactMutationScope scope,
        ExactChunkCoordinate target,
        CompletableFuture<Lease> result
    ) {
        requireServerThread();
        try {
            scope.requireChunkAllowed(target);
            if (closing || !runtime.available()) {
                throw new IllegalStateException("History chunk loader is unavailable");
            }
            Entry entry = entries.get(target);
            if (entry == null) {
                entry = new Entry(target);
                entries.put(target, entry);
                loadQueue.addLast(entry);
            }
            entry.waiters.addLast(result);
            if (entry.chunk != null && entry.activeLeaseId == 0L) {
                grantNext(entry);
            }
            pumpLoads();
        } catch (RuntimeException exception) {
            result.completeExceptionally(exception);
        }
    }

    private void pumpLoads() {
        requireServerThread();
        while (!closing && activeEntries < maximumConcurrentLeases && !loadQueue.isEmpty()) {
            Entry entry = loadQueue.removeFirst();
            if (entries.get(entry.coordinate) != entry || entry.loading || entry.chunk != null) {
                continue;
            }
            discardCompletedWaiters(entry);
            if (entry.waiters.isEmpty()) {
                entries.remove(entry.coordinate, entry);
                continue;
            }
            beginLoad(entry);
        }
    }

    private void beginLoad(Entry entry) {
        requireServerThread();
        entry.loading = true;
        entry.permitHeld = true;
        activeEntries++;
        CompletableFuture<ChunkAccess> load;
        try {
            load = runtime.load(entry.coordinate, generateMissingChunks)
                .orTimeout(loadTimeoutSeconds, TimeUnit.SECONDS);
        } catch (RuntimeException exception) {
            finishLoad(entry, null, exception);
            return;
        }
        load.whenComplete((chunk, failure) -> runOnServerThread(
            () -> finishLoad(entry, chunk, failure),
            null
        ));
    }

    private void finishLoad(Entry entry, ChunkAccess chunk, Throwable failure) {
        requireServerThread();
        if (!entry.loading) {
            return;
        }
        entry.loading = false;
        if (entries.get(entry.coordinate) != entry || closing) {
            releasePermit(entry);
            pumpLoads();
            return;
        }
        if (failure != null) {
            failLoad(entry, rootCause(failure));
            pumpLoads();
            return;
        }
        try {
            requireExactChunk(entry.coordinate, chunk);
            boolean added = chunk.addTicket();
            if (!added && !chunk.hasTicket()) {
                throw new IllegalStateException("Paper refused the History chunk ticket");
            }
            entry.chunk = chunk;
            entry.removeTicketOnClose = added;
            grantNext(entry);
        } catch (RuntimeException exception) {
            failLoad(entry, exception);
        }
        pumpLoads();
    }

    private void grantNext(Entry entry) {
        requireServerThread();
        if (entry.chunk == null || entry.activeLeaseId != 0L) {
            return;
        }
        discardCompletedWaiters(entry);
        if (entry.waiters.isEmpty()) {
            cleanup(entry);
            return;
        }
        CompletableFuture<Lease> waiter = entry.waiters.removeFirst();
        long leaseId = ++entry.lastLeaseId;
        entry.activeLeaseId = leaseId;
        waiter.complete(new Lease(this, entry.coordinate, entry.chunk, leaseId));
    }

    private void release(ExactChunkCoordinate coordinate, long leaseId) {
        runOnServerThread(() -> {
            Entry entry = entries.get(coordinate);
            if (entry == null || entry.activeLeaseId != leaseId) {
                return;
            }
            entry.activeLeaseId = 0L;
            grantNext(entry);
            cleanup(entry);
            pumpLoads();
        }, null);
    }

    private void cleanup(Entry entry) {
        requireServerThread();
        if (entry.loading || entry.activeLeaseId != 0L || !entry.waiters.isEmpty()) {
            return;
        }
        removeOwnedTicket(entry);
        releasePermit(entry);
        entries.remove(entry.coordinate, entry);
    }

    private void failLoad(Entry entry, Throwable failure) {
        requireServerThread();
        while (!entry.waiters.isEmpty()) {
            entry.waiters.removeFirst().completeExceptionally(failure);
        }
        removeOwnedTicket(entry);
        releasePermit(entry);
        entries.remove(entry.coordinate, entry);
    }

    private void removeOwnedTicket(Entry entry) {
        if (entry.chunk != null && entry.removeTicketOnClose) {
            entry.chunk.removeTicket();
        }
        entry.chunk = null;
        entry.removeTicketOnClose = false;
    }

    private void releasePermit(Entry entry) {
        if (!entry.permitHeld) {
            return;
        }
        entry.permitHeld = false;
        activeEntries--;
        if (activeEntries < 0) {
            throw new IllegalStateException("Chunk lease permit count became negative");
        }
    }

    private static void discardCompletedWaiters(Entry entry) {
        entry.waiters.removeIf(CompletableFuture::isDone);
    }

    private static void requireExactChunk(ExactChunkCoordinate expected, ChunkAccess actual) {
        if (actual == null || !actual.coordinate().equals(expected)) {
            throw new IllegalStateException("Paper returned a chunk outside the exact rollback scope");
        }
    }

    private void runOnServerThread(Runnable action, CompletableFuture<?> failureTarget) {
        if (runtime.isServerThread()) {
            invoke(action, failureTarget);
            return;
        }
        if (!runtime.available()) {
            if (failureTarget != null) {
                failureTarget.completeExceptionally(new IllegalStateException("History is disabled"));
            }
            return;
        }
        try {
            runtime.execute(() -> invoke(action, failureTarget));
        } catch (RuntimeException exception) {
            if (failureTarget != null) {
                failureTarget.completeExceptionally(exception);
            } else {
                runtime.warn("History chunk lease scheduling failed: " + exception.getMessage());
            }
        }
    }

    private void invoke(Runnable action, CompletableFuture<?> failureTarget) {
        try {
            action.run();
        } catch (RuntimeException exception) {
            if (failureTarget != null) {
                failureTarget.completeExceptionally(exception);
            } else {
                runtime.warn("History chunk lease callback failed: " + exception.getMessage());
            }
        }
    }

    private void requireServerThread() {
        if (!runtime.isServerThread()) {
            throw new IllegalStateException("Chunk lease state must be accessed on the server thread");
        }
    }

    private static Throwable rootCause(Throwable failure) {
        Throwable current = failure;
        while ((current instanceof CompletionException || current instanceof java.util.concurrent.ExecutionException)
            && current.getCause() != null) {
            current = current.getCause();
        }
        return current;
    }

    interface Runtime {
        boolean isServerThread();

        boolean available();

        void execute(Runnable action);

        CompletableFuture<ChunkAccess> load(ExactChunkCoordinate coordinate, boolean generate);

        void warn(String message);
    }

    interface ChunkAccess {
        ExactChunkCoordinate coordinate();

        Chunk chunk();

        boolean addTicket();

        boolean hasTicket();

        void removeTicket();
    }

    static final class Lease implements AutoCloseable {
        private final ScopedChunkLeaseManager owner;
        private final ExactChunkCoordinate coordinate;
        private final ChunkAccess chunk;
        private final long leaseId;
        private boolean closed;

        private Lease(
            ScopedChunkLeaseManager owner,
            ExactChunkCoordinate coordinate,
            ChunkAccess chunk,
            long leaseId
        ) {
            this.owner = owner;
            this.coordinate = coordinate;
            this.chunk = chunk;
            this.leaseId = leaseId;
        }

        Chunk chunk() {
            if (closed) {
                throw new IllegalStateException("Chunk lease is already closed");
            }
            requireExactChunk(coordinate, chunk);
            return chunk.chunk();
        }

        ExactChunkCoordinate coordinate() {
            return coordinate;
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            owner.release(coordinate, leaseId);
        }
    }

    private static final class Entry {
        private final ExactChunkCoordinate coordinate;
        private final ArrayDeque<CompletableFuture<Lease>> waiters = new ArrayDeque<>();
        private ChunkAccess chunk;
        private boolean loading;
        private boolean removeTicketOnClose;
        private boolean permitHeld;
        private long lastLeaseId;
        private long activeLeaseId;

        private Entry(ExactChunkCoordinate coordinate) {
            this.coordinate = coordinate;
        }
    }

    private static final class PaperRuntime implements Runtime {
        private final JavaPlugin plugin;

        private PaperRuntime(JavaPlugin plugin) {
            this.plugin = plugin;
        }

        @Override
        public boolean isServerThread() {
            return Bukkit.isPrimaryThread();
        }

        @Override
        public boolean available() {
            return plugin.isEnabled();
        }

        @Override
        public void execute(Runnable action) {
            Bukkit.getScheduler().runTask(plugin, action);
        }

        @Override
        public CompletableFuture<ChunkAccess> load(ExactChunkCoordinate coordinate, boolean generate) {
            World world = Bukkit.getWorld(coordinate.worldId());
            if (world == null) {
                return CompletableFuture.failedFuture(
                    new IllegalStateException("Rollback world is not loaded: " + coordinate.worldId())
                );
            }
            return world.getChunkAtAsync(coordinate.x(), coordinate.z(), generate)
                .thenApply(chunk -> new PaperChunkAccess(plugin, chunk));
        }

        @Override
        public void warn(String message) {
            plugin.getLogger().warning(message);
        }
    }

    private static final class PaperChunkAccess implements ChunkAccess {
        private final JavaPlugin plugin;
        private final Chunk chunk;
        private final ExactChunkCoordinate coordinate;

        private PaperChunkAccess(JavaPlugin plugin, Chunk chunk) {
            this.plugin = plugin;
            this.chunk = Objects.requireNonNull(chunk, "chunk");
            this.coordinate = new ExactChunkCoordinate(
                chunk.getWorld().getUID(),
                chunk.getX(),
                chunk.getZ()
            );
        }

        @Override
        public ExactChunkCoordinate coordinate() {
            return coordinate;
        }

        @Override
        public Chunk chunk() {
            return chunk;
        }

        @Override
        public boolean addTicket() {
            return chunk.addPluginChunkTicket(plugin);
        }

        @Override
        public boolean hasTicket() {
            return chunk.getPluginChunkTickets().contains(plugin);
        }

        @Override
        public void removeTicket() {
            chunk.removePluginChunkTicket(plugin);
        }
    }
}
