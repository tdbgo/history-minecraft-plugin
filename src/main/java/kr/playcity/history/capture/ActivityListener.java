package kr.playcity.history.capture;

import io.papermc.paper.event.player.AsyncChatEvent;
import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ActorKind;
import kr.playcity.history.model.ActorRef;
import kr.playcity.history.model.ChangeCause;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.bukkit.Location;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.entity.EntityDeathEvent;
import org.bukkit.event.entity.EntityPickupItemEvent;
import org.bukkit.event.entity.EntityPlaceEvent;
import org.bukkit.event.player.PlayerCommandPreprocessEvent;
import org.bukkit.event.player.PlayerDropItemEvent;
import org.bukkit.event.player.PlayerJoinEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.inventory.ItemStack;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.List;
import java.util.Locale;

public final class ActivityListener implements Listener {
    private final JavaPlugin plugin;
    private final HistoryConfig.Logging config;
    private final ActorResolver actors;
    private final ChangeRecorder recorder;

    public ActivityListener(
        JavaPlugin plugin,
        HistoryConfig.Logging config,
        ActorResolver actors,
        ChangeRecorder recorder
    ) {
        this.plugin = plugin;
        this.config = config;
        this.actors = actors;
        this.recorder = recorder;
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onJoin(PlayerJoinEvent event) {
        if (config.playerSessions()) {
            Player player = event.getPlayer();
            recorder.recordAudit(
                player.getLocation(),
                actors.player(player),
                ChangeCause.PLAYER_SESSION,
                "join"
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR)
    public void onQuit(PlayerQuitEvent event) {
        if (config.playerSessions()) {
            Player player = event.getPlayer();
            recorder.recordAudit(
                player.getLocation(),
                actors.player(player),
                ChangeCause.PLAYER_SESSION,
                "quit:" + event.getReason().name().toLowerCase(Locale.ROOT)
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onCommand(PlayerCommandPreprocessEvent event) {
        if (!config.playerCommands()) {
            return;
        }
        String command = event.getMessage().stripLeading();
        recorder.recordAudit(
            event.getPlayer().getLocation(),
            actors.player(event.getPlayer()),
            ChangeCause.PLAYER_COMMAND,
            "command:" + CommandRedactor.redact(command, config.redactedCommandPrefixes())
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onChat(AsyncChatEvent event) {
        if (!config.playerMessages()) {
            return;
        }
        String message = PlainTextComponentSerializer.plainText().serialize(event.message());
        plugin.getServer().getScheduler().runTask(plugin, () -> recorder.recordAudit(
            event.getPlayer().getLocation(),
            actors.player(event.getPlayer()),
            ChangeCause.PLAYER_MESSAGE,
            "message:" + message
        ));
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onDrop(PlayerDropItemEvent event) {
        if (config.items()) {
            ItemStack item = event.getItemDrop().getItemStack();
            recorder.recordAudit(
                event.getItemDrop().getLocation(),
                actors.player(event.getPlayer()),
                ChangeCause.ITEM_DROP,
                item.getType().getKey().asString(),
                itemSummary(item)
            );
        }
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onPickup(EntityPickupItemEvent event) {
        if (!config.items() || !(event.getEntity() instanceof Player player)) {
            return;
        }
        recorder.recordAudit(
            event.getItem().getLocation(),
            actors.player(player),
            ChangeCause.ITEM_PICKUP,
            event.getItem().getItemStack().getType().getKey().asString(),
            itemSummary(event.getItem().getItemStack())
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityPlace(EntityPlaceEvent event) {
        if (!config.entities()) {
            return;
        }
        Player player = event.getPlayer();
        ActorRef actor = player == null ? actors.entity(event.getEntity()) : actors.player(player);
        recorder.recordAudit(
            event.getEntity().getLocation(),
            actor,
            ChangeCause.ENTITY_PLACE,
            event.getEntityType().getKey().asString(),
            "entity:" + event.getEntityType().getKey().asString()
        );
    }

    @EventHandler(priority = EventPriority.MONITOR, ignoreCancelled = true)
    public void onEntityDeath(EntityDeathEvent event) {
        if (!config.entities()) {
            return;
        }
        Entity causing = event.getDamageSource().getCausingEntity();
        if (causing == null) {
            return;
        }
        ActorRef actor = actors.entity(causing);
        if (actor.kind() != ActorKind.PLAYER) {
            return;
        }
        Location location = event.getEntity().getLocation();
        recorder.recordAudit(
            location,
            actor,
            ChangeCause.ENTITY_KILL,
            event.getEntityType().getKey().asString(),
            "entity:" + event.getEntityType().getKey().asString()
        );
    }

    private static String itemSummary(ItemStack item) {
        return "item:" + item.getType().getKey().asString() + ";amount:" + item.getAmount();
    }
}
