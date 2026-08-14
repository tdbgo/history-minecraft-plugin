package kr.playcity.history.rollback;

import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.OperationItem;
import org.bukkit.Chunk;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ScopedChunkLeaseManagerTest {
    private static final UUID WORLD = UUID.fromString("44000000-0000-0000-0000-000000000004");
    private static final ExactChunkCoordinate FIRST = new ExactChunkCoordinate(WORLD, 0, 0);
    private static final ExactChunkCoordinate DISTANT = new ExactChunkCoordinate(WORLD, 100_000, -100_000);

    @Test
    void holdsTheGlobalPermitUntilTheResidentLeaseCloses() {
        FakeRuntime runtime = new FakeRuntime();
        ScopedChunkLeaseManager manager = new ScopedChunkLeaseManager(runtime, 1, 30, true);
        ExactMutationScope scope = scope();

        CompletableFuture<ScopedChunkLeaseManager.Lease> first = manager.acquire(scope, FIRST);
        CompletableFuture<ScopedChunkLeaseManager.Lease> distant = manager.acquire(scope, DISTANT);

        assertEquals(List.of(FIRST), runtime.loadRequests);
        FakeChunkAccess firstChunk = runtime.complete(FIRST);
        ScopedChunkLeaseManager.Lease firstLease = first.join();
        assertFalse(distant.isDone());
        assertEquals(List.of(FIRST), runtime.loadRequests);
        assertEquals(1, firstChunk.addedTickets);

        firstLease.close();
        assertEquals(List.of(FIRST, DISTANT), runtime.loadRequests);
        assertEquals(1, firstChunk.removedTickets);

        FakeChunkAccess distantChunk = runtime.complete(DISTANT);
        ScopedChunkLeaseManager.Lease distantLease = distant.join();
        assertEquals(DISTANT, distantLease.coordinate());
        distantLease.close();
        assertEquals(1, distantChunk.removedTickets);
    }

    @Test
    void serializesTwoOperationsForTheSameExactChunk() {
        FakeRuntime runtime = new FakeRuntime();
        ScopedChunkLeaseManager manager = new ScopedChunkLeaseManager(runtime, 4, 30, true);
        ExactMutationScope scope = scope();

        CompletableFuture<ScopedChunkLeaseManager.Lease> first = manager.acquire(scope, FIRST);
        CompletableFuture<ScopedChunkLeaseManager.Lease> second = manager.acquire(scope, FIRST);
        FakeChunkAccess chunk = runtime.complete(FIRST);

        ScopedChunkLeaseManager.Lease firstLease = first.join();
        assertFalse(second.isDone());
        assertEquals(1, runtime.loadRequests.size());
        assertEquals(1, chunk.addedTickets);

        firstLease.close();
        ScopedChunkLeaseManager.Lease secondLease = second.join();
        assertEquals(FIRST, secondLease.coordinate());
        assertEquals(0, chunk.removedTickets);

        secondLease.close();
        assertEquals(1, chunk.removedTickets);
    }

    @Test
    void loadFailureReleasesThePermitAndStartsTheNextExactChunk() {
        FakeRuntime runtime = new FakeRuntime();
        ScopedChunkLeaseManager manager = new ScopedChunkLeaseManager(runtime, 1, 30, true);
        ExactMutationScope scope = scope();

        CompletableFuture<ScopedChunkLeaseManager.Lease> first = manager.acquire(scope, FIRST);
        CompletableFuture<ScopedChunkLeaseManager.Lease> distant = manager.acquire(scope, DISTANT);
        runtime.fail(FIRST, new IllegalStateException("fixture-load-failure"));

        CompletionException failure = assertThrows(CompletionException.class, first::join);
        assertEquals("fixture-load-failure", failure.getCause().getMessage());
        assertEquals(List.of(FIRST, DISTANT), runtime.loadRequests);

        ScopedChunkLeaseManager.Lease lease = runtime.complete(DISTANT).leaseFrom(distant);
        lease.close();
    }

    @Test
    void closeFailsWaitersAndRemovesEveryOwnedTicket() {
        FakeRuntime runtime = new FakeRuntime();
        ScopedChunkLeaseManager manager = new ScopedChunkLeaseManager(runtime, 1, 30, true);
        ExactMutationScope scope = scope();

        CompletableFuture<ScopedChunkLeaseManager.Lease> active = manager.acquire(scope, FIRST);
        CompletableFuture<ScopedChunkLeaseManager.Lease> waiting = manager.acquire(scope, DISTANT);
        FakeChunkAccess chunk = runtime.complete(FIRST);
        assertTrue(active.isDone());

        manager.close();

        assertEquals(1, chunk.removedTickets);
        assertThrows(CompletionException.class, waiting::join);
        assertThrows(CompletionException.class, () -> manager.acquire(scope, FIRST).join());
    }

    @Test
    void rejectsAChunkThatWasNotNamedByTheMutationScope() {
        FakeRuntime runtime = new FakeRuntime();
        ScopedChunkLeaseManager manager = new ScopedChunkLeaseManager(runtime, 1, 30, true);
        ExactChunkCoordinate gap = new ExactChunkCoordinate(WORLD, 1, 0);

        assertThrows(IllegalStateException.class, () -> manager.acquire(scope(), gap));
        assertTrue(runtime.loadRequests.isEmpty());
    }

    @Test
    void rejectsAChunkReturnedForDifferentCoordinates() {
        FakeRuntime runtime = new FakeRuntime();
        ScopedChunkLeaseManager manager = new ScopedChunkLeaseManager(runtime, 1, 30, true);
        CompletableFuture<ScopedChunkLeaseManager.Lease> lease = manager.acquire(scope(), FIRST);

        runtime.completeAs(FIRST, DISTANT);

        assertThrows(CompletionException.class, lease::join);
    }

    private static ExactMutationScope scope() {
        return ExactMutationScope.create(List.of(
            item(0, 1, 64, 1),
            item(1, DISTANT.x() << 4, 70, DISTANT.z() << 4)
        ), 10, 2);
    }

    private static OperationItem item(int sequence, int x, int y, int z) {
        return new OperationItem(
            sequence,
            new BlockPosition(WORLD, x, y, z),
            BlockSnapshot.block("minecraft:stone"),
            BlockSnapshot.air(),
            List.of((long) sequence + 1L)
        );
    }

    private static final class FakeRuntime implements ScopedChunkLeaseManager.Runtime {
        private final List<ExactChunkCoordinate> loadRequests = new ArrayList<>();
        private final Map<ExactChunkCoordinate, CompletableFuture<ScopedChunkLeaseManager.ChunkAccess>> pending
            = new HashMap<>();
        private boolean available = true;

        @Override
        public boolean isServerThread() {
            return true;
        }

        @Override
        public boolean available() {
            return available;
        }

        @Override
        public void execute(Runnable action) {
            action.run();
        }

        @Override
        public CompletableFuture<ScopedChunkLeaseManager.ChunkAccess> load(
            ExactChunkCoordinate coordinate,
            boolean generate
        ) {
            assertTrue(generate);
            loadRequests.add(coordinate);
            CompletableFuture<ScopedChunkLeaseManager.ChunkAccess> future = new CompletableFuture<>();
            pending.put(coordinate, future);
            return future;
        }

        @Override
        public void warn(String message) {
            throw new AssertionError(message);
        }

        private FakeChunkAccess complete(ExactChunkCoordinate requested) {
            return completeAs(requested, requested);
        }

        private FakeChunkAccess completeAs(
            ExactChunkCoordinate requested,
            ExactChunkCoordinate returned
        ) {
            FakeChunkAccess chunk = new FakeChunkAccess(returned);
            pending.remove(requested).complete(chunk);
            return chunk;
        }

        private void fail(ExactChunkCoordinate requested, Throwable failure) {
            pending.remove(requested).completeExceptionally(failure);
        }
    }

    private static final class FakeChunkAccess implements ScopedChunkLeaseManager.ChunkAccess {
        private final ExactChunkCoordinate coordinate;
        private int addedTickets;
        private int removedTickets;
        private boolean ticket;

        private FakeChunkAccess(ExactChunkCoordinate coordinate) {
            this.coordinate = coordinate;
        }

        @Override
        public ExactChunkCoordinate coordinate() {
            return coordinate;
        }

        @Override
        public Chunk chunk() {
            return null;
        }

        @Override
        public boolean addTicket() {
            addedTickets++;
            ticket = true;
            return true;
        }

        @Override
        public boolean hasTicket() {
            return ticket;
        }

        @Override
        public void removeTicket() {
            removedTickets++;
            ticket = false;
        }

        private ScopedChunkLeaseManager.Lease leaseFrom(
            CompletableFuture<ScopedChunkLeaseManager.Lease> future
        ) {
            return future.join();
        }
    }
}
