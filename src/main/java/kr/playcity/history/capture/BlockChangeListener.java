package kr.playcity.history.capture;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.data.Waterlogged;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.BlockBurnEvent;
import org.bukkit.event.block.BlockExplodeEvent;
import org.bukkit.event.block.BlockFadeEvent;
import org.bukkit.event.block.BlockFormEvent;
import org.bukkit.event.block.BlockFromToEvent;
import org.bukkit.event.block.BlockGrowEvent;
import org.bukkit.event.block.BlockMultiPlaceEvent;
import org.bukkit.event.block.BlockPistonExtendEvent;
import org.bukkit.event.block.BlockPistonRetractEvent;
import org.bukkit.event.block.BlockPlaceEvent;
import org.bukkit.event.block.BlockBreakEvent;
import org.bukkit.event.block.BlockSpreadEvent;
import org.bukkit.event.block.EntityBlockFormEvent;
import org.bukkit.event.entity.EntityChangeBlockEvent;
import org.bukkit.event.entity.EntityExplodeEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class BlockChangeListener implements Listener {
    private final JavaPlugin plugin;
    private final HistoryConfig.Logging config;
    private final SnapshotCodec snapshots;
    private final ActorResolver actors;
    private final ChangeRecorder recorder;

    public BlockChangeListener(
        JavaPlugin plugin,
        HistoryConfig.Logging config,
        SnapshotCodec snapshots,
        ActorResolver actors,
        ChangeRecorder recorder
    ) {
        this.plugin = plugin;
        this.config = config;
        this.snapshots = snapshots;
        this.actors = actors;
        this.recorder = recorder;
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPlace(BlockPlaceEvent event) {
        if (!config.playerBlocks() || event instanceof BlockMultiPlaceEvent) {
            return;
        }
        recorder.record(
            event.getBlockPlaced(),
            actors.player(event.getPlayer()),
            ChangeCause.PLAYER_PLACE,
            snapshots.capture(event.getBlockReplacedState()),
            snapshots.capture(event.getBlockPlaced()),
            ""
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onMultiPlace(BlockMultiPlaceEvent event) {
        if (!config.playerBlocks()) {
            return;
        }
        ActorRef actor = actors.player(event.getPlayer());
        for (BlockState replaced : event.getReplacedBlockStates()) {
            Block placed = replaced.getBlock();
            recorder.record(
                placed,
                actor,
                ChangeCause.PLAYER_PLACE,
                snapshots.capture(replaced),
                snapshots.capture(placed),
                "multi-place"
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBreak(BlockBreakEvent event) {
        if (!config.playerBlocks()) {
            return;
        }
        recordDestroyedBlocks(
            List.of(event.getBlock()),
            actors.player(event.getPlayer()),
            ChangeCause.PLAYER_BREAK,
            ""
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityExplosion(EntityExplodeEvent event) {
        if (!config.explosions()) {
            return;
        }
        recordDestroyedBlocks(
            event.blockList(),
            actors.entity(event.getEntity()),
            ChangeCause.EXPLOSION,
            "entity"
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBlockExplosion(BlockExplodeEvent event) {
        if (!config.explosions()) {
            return;
        }
        recordDestroyedBlocks(
            event.blockList(),
            ActorRef.entity("#" + event.getBlock().getType().getKey().getKey()),
            ChangeCause.EXPLOSION,
            "block"
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBurn(BlockBurnEvent event) {
        if (config.naturalChanges()) {
            recordDestroyedBlocks(
                List.of(event.getBlock()),
                ActorRef.natural("#fire"),
                ChangeCause.FIRE,
                ""
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onFade(BlockFadeEvent event) {
        if (config.naturalChanges()) {
            recordStateTransition(event.getBlock(), event.getNewState(), ActorRef.natural("#fade"), ChangeCause.FADE);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onGrow(BlockGrowEvent event) {
        if (config.naturalChanges() && !(event instanceof BlockFormEvent)) {
            recordStateTransition(event.getBlock(), event.getNewState(), ActorRef.natural("#growth"), ChangeCause.GROWTH);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onForm(BlockFormEvent event) {
        if (config.naturalChanges()
            && !(event instanceof BlockSpreadEvent)
            && !(event instanceof EntityBlockFormEvent)) {
            recordStateTransition(event.getBlock(), event.getNewState(), ActorRef.natural("#form"), ChangeCause.FORM);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityForm(EntityBlockFormEvent event) {
        if (config.naturalChanges()) {
            recordStateTransition(
                event.getBlock(),
                event.getNewState(),
                actors.entity(event.getEntity()),
                ChangeCause.FORM
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onSpread(BlockSpreadEvent event) {
        if (config.naturalChanges()) {
            recordStateTransition(event.getBlock(), event.getNewState(), ActorRef.natural("#spread"), ChangeCause.SPREAD);
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onLiquid(BlockFromToEvent event) {
        if (!config.liquids()) {
            return;
        }
        Block destination = event.getToBlock();
        BlockSnapshot before = snapshots.capture(destination);
        ActorRef actor = ActorRef.natural("#" + event.getBlock().getType().getKey().getKey());
        plugin.getServer().getScheduler().runTask(plugin, () -> recorder.record(
            destination,
            actor,
            ChangeCause.LIQUID,
            before,
            snapshots.capture(destination),
            "flow"
        ));
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityChangeBlock(EntityChangeBlockEvent event) {
        recorder.record(
            event.getBlock(),
            actors.entity(event.getEntity()),
            ChangeCause.ENTITY_CHANGE,
            snapshots.capture(event.getBlock()),
            BlockSnapshot.block(event.getTo().createBlockData().getAsString(false)),
            event.getEntityType().getKey().getKey()
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPistonExtend(BlockPistonExtendEvent event) {
        if (config.pistons()) {
            recordPistonMovement(
                event.getBlock(),
                event.getBlocks(),
                event.getDirection(),
                event.getDirection(),
                "extend"
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPistonRetract(BlockPistonRetractEvent event) {
        if (config.pistons()) {
            recordPistonMovement(
                event.getBlock(),
                event.getBlocks(),
                event.getDirection().getOppositeFace(),
                event.getDirection(),
                "retract"
            );
        }
    }

    private void recordStateTransition(
        Block block,
        BlockState newState,
        ActorRef actor,
        ChangeCause cause
    ) {
        recorder.record(block, actor, cause, snapshots.capture(block), snapshots.capture(newState), "");
    }

    private void recordDestroyedBlocks(
        List<Block> blocks,
        ActorRef actor,
        ChangeCause cause,
        String metadata
    ) {
        Map<BlockPosition, Block> unique = new LinkedHashMap<>();
        for (Block block : blocks) {
            unique.putIfAbsent(position(block), block);
        }
        for (Map.Entry<BlockPosition, Block> entry : unique.entrySet()) {
            Block block = entry.getValue();
            BlockSnapshot before = snapshots.capture(block);
            BlockSnapshot after = block.getBlockData() instanceof Waterlogged waterlogged && waterlogged.isWaterlogged()
                ? BlockSnapshot.block("minecraft:water[level=0]")
                : BlockSnapshot.air();
            recorder.record(entry.getKey(), actor, cause, before, after, metadata);
        }
    }

    private void recordPistonMovement(
        Block piston,
        List<Block> movedBlocks,
        org.bukkit.block.BlockFace movementDirection,
        org.bukkit.block.BlockFace pistonFacing,
        String metadata
    ) {
        Map<BlockPosition, Block> blocks = new LinkedHashMap<>();
        Map<BlockPosition, BlockSnapshot> before = new LinkedHashMap<>();

        rememberPistonBlock(piston, blocks, before);
        rememberPistonBlock(piston.getRelative(pistonFacing), blocks, before);

        for (Block source : movedBlocks) {
            rememberPistonBlock(source, blocks, before);
            rememberPistonBlock(source.getRelative(movementDirection), blocks, before);
        }

        ActorRef actor = ActorRef.natural("#piston");
        plugin.getServer().getScheduler().runTask(plugin, () -> {
            for (Map.Entry<BlockPosition, BlockSnapshot> entry : before.entrySet()) {
                recorder.record(
                    entry.getKey(),
                    actor,
                    ChangeCause.PISTON,
                    entry.getValue(),
                    snapshots.capture(blocks.get(entry.getKey())),
                    metadata
                );
            }
        });
    }

    private void rememberPistonBlock(
        Block block,
        Map<BlockPosition, Block> blocks,
        Map<BlockPosition, BlockSnapshot> before
    ) {
        BlockPosition position = position(block);
        blocks.putIfAbsent(position, block);
        before.computeIfAbsent(position, ignored -> snapshots.capture(block));
    }

    private static BlockPosition position(Block block) {
        return new BlockPosition(block.getWorld().getUID(), block.getX(), block.getY(), block.getZ());
    }
}
