package kr.playcity.history.capture;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.BlockPosition;
import kr.playcity.history.model.BlockSnapshot;
import kr.playcity.history.model.ChangeCause;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.DoubleChest;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.inventory.InventoryClickEvent;
import org.bukkit.event.inventory.InventoryDragEvent;
import org.bukkit.event.inventory.InventoryMoveItemEvent;
import org.bukkit.event.inventory.InventoryPickupItemEvent;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.LinkedHashMap;
import java.util.Map;

public final class ContainerChangeListener implements Listener {
    private final JavaPlugin plugin;
    private final HistoryConfig.Logging config;
    private final SnapshotCodec snapshots;
    private final ActorResolver actors;
    private final ChangeRecorder recorder;
    private final Map<BlockPosition, PendingContainer> pending = new LinkedHashMap<>();
    private boolean flushScheduled;

    public ContainerChangeListener(
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
    public void onClick(InventoryClickEvent event) {
        if (!config.containers()) {
            return;
        }
        ActorRef actor = event.getWhoClicked() instanceof Player player
            ? actors.player(player)
            : actors.entity(event.getWhoClicked());
        queue(event.getView().getTopInventory(), actor, "click");
        if (event.getClickedInventory() != null) {
            queue(event.getClickedInventory(), actor, "click");
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onDrag(InventoryDragEvent event) {
        if (!config.containers()) {
            return;
        }
        ActorRef actor = event.getWhoClicked() instanceof Player player
            ? actors.player(player)
            : actors.entity(event.getWhoClicked());
        queue(event.getView().getTopInventory(), actor, "drag");
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onMove(InventoryMoveItemEvent event) {
        if (!config.containers()) {
            return;
        }
        ActorRef actor = ActorRef.natural("#inventory-move");
        String metadata = "move:" + itemKey(event.getItem());
        queue(event.getSource(), actor, metadata);
        queue(event.getDestination(), actor, metadata);
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPickup(InventoryPickupItemEvent event) {
        if (config.containers()) {
            queue(
                event.getInventory(),
                ActorRef.natural("#inventory-pickup"),
                "pickup:" + itemKey(event.getItem().getItemStack())
            );
        }
    }

    private void queue(Inventory inventory, ActorRef actor, String metadata) {
        addHolder(inventory.getHolder(false), actor, metadata);
        if (!flushScheduled && !pending.isEmpty()) {
            flushScheduled = true;
            plugin.getServer().getScheduler().runTask(plugin, this::flush);
        }
    }

    private void addHolder(InventoryHolder holder, ActorRef actor, String metadata) {
        if (holder instanceof DoubleChest doubleChest) {
            addHolder(doubleChest.getLeftSide(false), actor, metadata);
            addHolder(doubleChest.getRightSide(false), actor, metadata);
            return;
        }
        if (!(holder instanceof BlockState state)) {
            return;
        }
        Block block = state.getBlock();
        BlockPosition position = position(block);
        pending.computeIfAbsent(
            position,
            ignored -> new PendingContainer(block, snapshots.capture(block), actor, metadata)
        );
    }

    private void flush() {
        flushScheduled = false;
        Map<BlockPosition, PendingContainer> changes = new LinkedHashMap<>(pending);
        pending.clear();
        changes.forEach((position, change) -> recorder.record(
            position,
            change.actor(),
            ChangeCause.CONTAINER,
            change.before(),
            snapshots.capture(change.block()),
            change.metadata()
        ));
    }

    private static String itemKey(ItemStack item) {
        return item.getType().getKey().asString();
    }

    private static BlockPosition position(Block block) {
        return new BlockPosition(block.getWorld().getUID(), block.getX(), block.getY(), block.getZ());
    }

    private record PendingContainer(
        Block block,
        BlockSnapshot before,
        ActorRef actor,
        String metadata
    ) {
    }
}
