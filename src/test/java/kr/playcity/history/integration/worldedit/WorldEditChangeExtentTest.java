package kr.playcity.history.integration.worldedit;

import com.sk89q.worldedit.WorldEditException;
import com.sk89q.worldedit.entity.BaseEntity;
import com.sk89q.worldedit.entity.Entity;
import com.sk89q.worldedit.extent.Extent;
import com.sk89q.worldedit.function.operation.Operation;
import com.sk89q.worldedit.math.BlockVector3;
import com.sk89q.worldedit.regions.Region;
import com.sk89q.worldedit.util.Location;
import com.sk89q.worldedit.world.block.BaseBlock;
import com.sk89q.worldedit.world.block.BlockState;
import com.sk89q.worldedit.world.block.BlockStateHolder;
import com.sk89q.worldedit.world.block.BlockType;
import kr.playcity.history.capture.ChangeRecorder;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.model.OperationCompletion;
import kr.playcity.history.model.OperationDraft;
import kr.playcity.history.model.StoredOperation;
import kr.playcity.history.storage.HistoryStore;
import kr.playcity.history.storage.StoreStatus;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorldEditChangeExtentTest {
    @Test
    void capturesUpstreamAndFaweSetBlockPathsInOneBatch() throws Exception {
        BlockVector3 first = BlockVector3.at(-3, 64, 5);
        BlockVector3 second = BlockVector3.at(-2, 64, 5);
        BlockState stone = state("minecraft:stone");
        BlockState dirt = state("minecraft:dirt");
        MemoryExtent memory = new MemoryExtent();
        memory.setBlock(first, stone);
        memory.setBlock(second, stone);

        CapturingStore store = new CapturingStore();
        UUID worldId = UUID.randomUUID();
        UUID batchId = UUID.randomUUID();
        WorldEditChangeExtent extent = new WorldEditChangeExtent(
            memory,
            worldId,
            ActorRef.player(UUID.randomUUID(), "Builder"),
            batchId,
            new ChangeRecorder(store, Logger.getAnonymousLogger())
        );

        assertTrue(extent.setBlock(first, dirt));
        assertTrue(extent.setBlock(second.x(), second.y(), second.z(), dirt));

        assertEquals(2, store.changes.size());
        for (ChangeRecord change : store.changes) {
            assertEquals(worldId, change.position().worldId());
            assertEquals(ChangeCause.WORLD_EDIT, change.cause());
            assertEquals("minecraft:stone", change.before().blockData());
            assertEquals("minecraft:dirt", change.after().blockData());
            assertEquals(batchId, change.batchId());
            assertEquals("", change.metadata());
        }
    }

    @Test
    void preservesFallbackWorldEditWhenHistoryCannotRecordIt() throws Exception {
        BlockVector3 position = BlockVector3.at(4, 70, -8);
        BlockState stone = state("minecraft:stone");
        BlockState dirt = state("minecraft:dirt");
        MemoryExtent memory = new MemoryExtent();
        memory.setBlock(position, stone);
        CapturingStore store = new CapturingStore(false);
        WorldEditChangeExtent extent = new WorldEditChangeExtent(
            memory,
            UUID.randomUUID(),
            ActorRef.player(UUID.randomUUID(), "Builder"),
            UUID.randomUUID(),
            new ChangeRecorder(store, Logger.getAnonymousLogger())
        );

        assertTrue(extent.setBlock(position, dirt));

        assertEquals("minecraft:dirt", memory.getBlock(position).getAsString());
        assertTrue(store.changes.isEmpty());
    }

    @Test
    void preservesFallbackWorldEditWhenHistoryRecordingThrows() throws Exception {
        BlockVector3 position = BlockVector3.at(6, 71, -9);
        BlockState stone = state("minecraft:stone");
        BlockState dirt = state("minecraft:dirt");
        MemoryExtent memory = new MemoryExtent();
        memory.setBlock(position, stone);
        CapturingStore store = new CapturingStore(true, true);
        WorldEditChangeExtent extent = new WorldEditChangeExtent(
            memory,
            UUID.randomUUID(),
            ActorRef.player(UUID.randomUUID(), "Builder"),
            UUID.randomUUID(),
            new ChangeRecorder(store, Logger.getAnonymousLogger())
        );

        assertTrue(extent.setBlock(position, dirt));

        assertEquals("minecraft:dirt", memory.getBlock(position).getAsString());
        assertTrue(store.changes.isEmpty());
    }

    @Test
    void doesNotRecordWhenDelegateDoesNotChangeTheWorld() throws Exception {
        BlockVector3 position = BlockVector3.at(7, 80, 9);
        BlockState stone = state("minecraft:stone");
        BlockState dirt = state("minecraft:dirt");
        MemoryExtent memory = new MemoryExtent();
        memory.setBlock(position, stone);
        memory.acceptMutations = false;
        CapturingStore store = new CapturingStore();
        WorldEditChangeExtent extent = new WorldEditChangeExtent(
            memory,
            UUID.randomUUID(),
            ActorRef.player(UUID.randomUUID(), "Builder"),
            UUID.randomUUID(),
            new ChangeRecorder(store, Logger.getAnonymousLogger())
        );

        assertFalse(extent.setBlock(position, dirt));

        assertEquals("minecraft:stone", memory.getBlock(position).getAsString());
        assertTrue(store.changes.isEmpty());
    }

    private static BlockState state(String id) throws Exception {
        Constructor<BlockState> constructor = BlockState.class.getDeclaredConstructor(
            BlockType.class,
            Map.class,
            int.class
        );
        constructor.setAccessible(true);
        return constructor.newInstance(new BlockType(id), Map.of(), 0);
    }

    private static final class MemoryExtent implements Extent {
        private final Map<BlockVector3, BlockState> states = new HashMap<>();
        private boolean acceptMutations = true;

        @Override
        public BlockVector3 getMinimumPoint() {
            return BlockVector3.at(-30_000_000, -64, -30_000_000);
        }

        @Override
        public BlockVector3 getMaximumPoint() {
            return BlockVector3.at(30_000_000, 319, 30_000_000);
        }

        @Override
        public List<? extends Entity> getEntities(Region region) {
            return List.of();
        }

        @Override
        public List<? extends Entity> getEntities() {
            return List.of();
        }

        @Override
        public Entity createEntity(Location location, BaseEntity entity) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BlockState getBlock(BlockVector3 position) {
            BlockState state = states.get(position);
            if (state == null) {
                throw new IllegalStateException("No test block at " + position);
            }
            return state;
        }

        @Override
        public BaseBlock getFullBlock(BlockVector3 position) {
            return getBlock(position).toBaseBlock();
        }

        @Override
        public <T extends BlockStateHolder<T>> boolean setBlock(BlockVector3 position, T block)
            throws WorldEditException {
            if (!acceptMutations) {
                return false;
            }
            BlockState target = block.toImmutableState();
            BlockState previous = states.put(position, target);
            return previous == null || !previous.getAsString().equals(target.getAsString());
        }

        @Override
        public Operation commit() {
            return null;
        }
    }

    private static final class CapturingStore implements HistoryStore {
        private final List<ChangeRecord> changes = new ArrayList<>();
        private final boolean accepting;
        private final boolean throwOnRecord;

        private CapturingStore() {
            this(true, false);
        }

        private CapturingStore(boolean accepting) {
            this(accepting, false);
        }

        private CapturingStore(boolean accepting, boolean throwOnRecord) {
            this.accepting = accepting;
            this.throwOnRecord = throwOnRecord;
        }

        @Override
        public boolean append(ChangeRecord change) {
            if (!accepting) {
                return false;
            }
            changes.add(change);
            return true;
        }

        @Override
        public boolean tryAppendWorldEdit(ChangeRecord change) {
            if (throwOnRecord) {
                throw new IllegalStateException("injected History failure");
            }
            if (!accepting) {
                return false;
            }
            changes.add(change);
            return true;
        }

        @Override
        public CompletableFuture<List<ChangeRecord>> query(HistoryQuery query) {
            return CompletableFuture.completedFuture(List.copyOf(changes));
        }

        @Override
        public CompletableFuture<Void> prepareOperation(OperationDraft operation) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Void> completeOperation(OperationCompletion completion) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Optional<StoredOperation>> loadOperation(UUID operationId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public CompletableFuture<Optional<StoredOperation>> findLastOperation(UUID actorId) {
            return CompletableFuture.failedFuture(new UnsupportedOperationException());
        }

        @Override
        public StoreStatus status() {
            return new StoreStatus(
                "test",
                true,
                accepting,
                true,
                true,
                0,
                changes.size(),
                changes.size(),
                0,
                0,
                0,
                0,
                0,
                ""
            );
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            return CompletableFuture.completedFuture(null);
        }
    }
}
