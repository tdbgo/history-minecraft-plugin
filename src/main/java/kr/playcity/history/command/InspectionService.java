package kr.playcity.history.command;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.storage.HistoryStore;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.bukkit.Bukkit;
import org.bukkit.block.Block;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.EventPriority;
import org.bukkit.event.Listener;
import org.bukkit.event.block.Action;
import org.bukkit.event.player.PlayerInteractEvent;
import org.bukkit.event.player.PlayerQuitEvent;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class InspectionService implements Listener {
    private static final long QUERY_THROTTLE_MILLIS = 400L;
    private final JavaPlugin plugin;
    private final HistoryStore store;
    private final HistoryConfig.Inspection config;
    private final Set<UUID> enabled = ConcurrentHashMap.newKeySet();
    private final Map<UUID, Long> lastQuery = new ConcurrentHashMap<>();

    public InspectionService(JavaPlugin plugin, HistoryStore store, HistoryConfig.Inspection config) {
        this.plugin = plugin;
        this.store = store;
        this.config = config;
    }

    public boolean toggle(Player player) {
        UUID playerId = player.getUniqueId();
        if (enabled.remove(playerId)) {
            return false;
        }
        enabled.add(playerId);
        return true;
    }

    public boolean isEnabled(Player player) {
        return enabled.contains(player.getUniqueId());
    }

    @EventHandler(priority = EventPriority.LOWEST)
    public void onInteract(PlayerInteractEvent event) {
        Player player = event.getPlayer();
        if (!isEnabled(player) || !player.hasPermission("history.inspect")) {
            return;
        }
        Action action = event.getAction();
        if (action != Action.LEFT_CLICK_BLOCK && action != Action.RIGHT_CLICK_BLOCK) {
            return;
        }
        Block block = event.getClickedBlock();
        if (block == null) {
            return;
        }
        event.setCancelled(true);
        long now = System.currentTimeMillis();
        Long previous = lastQuery.put(player.getUniqueId(), now);
        if (previous != null && now - previous < QUERY_THROTTLE_MILLIS) {
            return;
        }

        HistoryQuery query = HistoryQuery.at(
            block.getWorld().getUID(),
            block.getX(),
            block.getY(),
            block.getZ(),
            0L,
            config.resultLimit()
        );
        player.sendActionBar(Component.text("이력을 찾는 중…", NamedTextColor.GRAY));
        store.query(query).whenComplete((changes, failure) -> {
            if (!plugin.isEnabled()) {
                return;
            }
            Bukkit.getScheduler().runTask(plugin, () -> {
                if (!player.isOnline()) {
                    return;
                }
                if (failure != null) {
                    player.sendMessage(HistoryUi.prefixed(Component.text(
                        "조회 실패: " + HistoryUi.userError(failure),
                        NamedTextColor.RED
                    )));
                    return;
                }
                player.sendMessage(HistoryUi.prefixed(Component.text(
                    block.getX() + ", " + block.getY() + ", " + block.getZ() + " 이력",
                    NamedTextColor.WHITE
                )));
                if (changes.isEmpty()) {
                    player.sendMessage(Component.text(" • 기록이 없습니다.", NamedTextColor.GRAY));
                } else {
                    changes.forEach(change -> player.sendMessage(HistoryUi.historyEntry(change)));
                }
            });
        });
    }

    @EventHandler
    public void onQuit(PlayerQuitEvent event) {
        enabled.remove(event.getPlayer().getUniqueId());
        lastQuery.remove(event.getPlayer().getUniqueId());
    }
}
