package kr.playcity.history.capture;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import org.bukkit.Material;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.entity.Player;
import org.bukkit.event.EventPriority;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.block.BlockFertilizeEvent;
import org.bukkit.event.block.CauldronLevelChangeEvent;
import org.bukkit.event.block.FluidLevelChangeEvent;
import org.bukkit.event.block.MoistureChangeEvent;
import org.bukkit.event.block.SignChangeEvent;
import org.bukkit.event.block.SpongeAbsorbEvent;
import org.bukkit.event.player.PlayerBucketEmptyEvent;
import org.bukkit.event.player.PlayerBucketFillEvent;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.world.PortalCreateEvent;
import org.bukkit.event.world.StructureGrowEvent;
import org.bukkit.inventory.EquipmentSlot;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public final class ExtendedBlockChangeListener implements Listener {
    private final JavaPlugin plugin;
    private final HistoryConfig.Logging config;
    private final SnapshotCodec snapshots;
    private final ActorResolver actors;
    private final ChangeRecorder recorder;

    public ExtendedBlockChangeListener(
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
    public void onInteract(PlayerInteractEvent event) {
        if (!config.playerInteractions() || event.getClickedBlock() == null) {
            return;
        }
        if (event.getAction() != Action.RIGHT_CLICK_BLOCK && event.getAction() != Action.PHYSICAL) {
            return;
        }
        if (event.getAction() == Action.RIGHT_CLICK_BLOCK && event.getHand() != EquipmentSlot.HAND) {
            return;
        }
        if (event.getMaterial().name().endsWith("_BUCKET")) {
            return;
        }
        recordActualNextTick(
            List.of(event.getClickedBlock()),
            actors.player(event.getPlayer()),
            ChangeCause.PLAYER_INTERACT,
            event.getAction().name().toLowerCase(java.util.Locale.ROOT)
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketEmpty(PlayerBucketEmptyEvent event) {
        if (config.playerInteractions()) {
            recordBucket(event.getBlock(), event.getPlayer(), "empty:" + material(event.getBucket()));
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onBucketFill(PlayerBucketFillEvent event) {
        if (config.playerInteractions()) {
            recordBucket(event.getBlock(), event.getPlayer(), "fill:" + material(event.getBucket()));
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onSignChange(SignChangeEvent event) {
        if (config.signs()) {
            recordActualNextTick(
                List.of(event.getBlock()),
                actors.player(event.getPlayer()),
                ChangeCause.SIGN,
                event.getSide().name().toLowerCase(java.util.Locale.ROOT)
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onFertilize(BlockFertilizeEvent event) {
        if (!config.naturalChanges()) {
            return;
        }
        ActorRef actor = event.getPlayer() == null
            ? ActorRef.natural("#fertilize")
            : actors.player(event.getPlayer());
        recordProposedStates(event.getBlocks(), actor, ChangeCause.GROWTH, "fertilize");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onStructureGrow(StructureGrowEvent event) {
        if (!config.naturalChanges() || event.isFromBonemeal()) {
            return;
        }
        ActorRef actor = event.getPlayer() == null
            ? ActorRef.natural("#structure")
            : actors.player(event.getPlayer());
        recordProposedStates(
            event.getBlocks(),
            actor,
            ChangeCause.GROWTH,
            event.getSpecies().name().toLowerCase(java.util.Locale.ROOT)
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPortalCreate(PortalCreateEvent event) {
        if (!config.naturalChanges()) {
            return;
        }
        ActorRef actor = event.getEntity() == null
            ? ActorRef.natural("#portal")
            : actors.entity(event.getEntity());
        recordProposedStates(
            event.getBlocks(),
            actor,
            ChangeCause.PORTAL,
            event.getReason().name().toLowerCase(java.util.Locale.ROOT)
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onSpongeAbsorb(SpongeAbsorbEvent event) {
        if (config.liquids()) {
            recordProposedStates(
                event.getBlocks(),
                ActorRef.natural("#sponge"),
                ChangeCause.LIQUID,
                "sponge"
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onFluidLevel(FluidLevelChangeEvent event) {
        if (!config.liquids()) {
            return;
        }
        Block block = event.getBlock();
        recorder.record(
            block,
            ActorRef.natural("#fluid"),
            ChangeCause.LIQUID,
            snapshots.capture(block),
            BlockSnapshot.block(event.getNewData().getAsString(false)),
            "level"
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onMoisture(MoistureChangeEvent event) {
        if (config.naturalChanges()) {
            recordState(event.getBlock(), event.getNewState(), ActorRef.natural("#moisture"), "moisture");
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onCauldron(CauldronLevelChangeEvent event) {
        if (!config.playerInteractions() && !config.naturalChanges()) {
            return;
        }
        ActorRef actor = event.getEntity() == null
            ? ActorRef.natural("#cauldron")
            : actors.entity(event.getEntity());
        recorder.record(
            event.getBlock(),
            actor,
            ChangeCause.LIQUID,
            snapshots.capture(event.getBlock()),
            snapshots.capture(event.getNewState()),
            "cauldron:" + event.getReason().name().toLowerCase(java.util.Locale.ROOT)
        );
    }

    private void recordBucket(Block block, Player player, String metadata) {
        recordActualNextTick(List.of(block), actors.player(player), ChangeCause.BUCKET, metadata);
    }

    private void recordState(Block block, BlockState after, ActorRef actor, String metadata) {
        recorder.record(
            block,
            actor,
            ChangeCause.FORM,
            snapshots.capture(block),
            snapshots.capture(after),
            metadata
        );
    }

    private void recordProposedStates(
        List<BlockState> states,
        ActorRef actor,
        ChangeCause cause,
        String metadata
    ) {
        for (BlockState after : states) {
            recorder.record(
                after,
                actor,
                cause,
                snapshots.capture(after.getBlock()),
                snapshots.capture(after),
                metadata
            );
        }
    }

    private void recordActualNextTick(
        List<Block> blocks,
        ActorRef actor,
        ChangeCause cause,
        String metadata
    ) {
        Map<BlockPosition, Block> unique = new LinkedHashMap<>();
        Map<BlockPosition, BlockSnapshot> before = new LinkedHashMap<>();
        for (Block block : blocks) {
            BlockPosition position = position(block);
            unique.putIfAbsent(position, block);
            before.computeIfAbsent(position, ignored -> snapshots.capture(block));
        }
        plugin.getServer().getScheduler().runTask(plugin, () -> before.forEach((position, snapshot) ->
            recorder.record(position, actor, cause, snapshot, snapshots.capture(unique.get(position)), metadata)
        ));
    }

    private static String material(Material material) {
        return material.getKey().asString();
    }

    private static BlockPosition position(Block block) {
        return new BlockPosition(block.getWorld().getUID(), block.getX(), block.getY(), block.getZ());
    }
}
