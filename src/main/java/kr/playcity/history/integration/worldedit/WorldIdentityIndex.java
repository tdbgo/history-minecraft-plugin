package kr.playcity.history.integration.worldedit;

import org.bukkit.Bukkit;
import org.bukkit.World;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.world.WorldLoadEvent;
import org.bukkit.event.world.WorldUnloadEvent;

import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Copies Bukkit world identity while on the server thread so FAWE workers never
 * need to call Bukkit APIs.
 */
public final class WorldIdentityIndex implements Listener {
    private final Map<String, UUID> worldIds = new ConcurrentHashMap<>();

    public WorldIdentityIndex() {
        Bukkit.getWorlds().forEach(this::remember);
    }

    public Optional<UUID> find(String worldName) {
        if (worldName == null || worldName.isBlank()) {
            return Optional.empty();
        }
        return Optional.ofNullable(worldIds.get(normalize(worldName)));
    }

    @EventHandler
    public void onWorldLoad(WorldLoadEvent event) {
        remember(event.getWorld());
    }

    @EventHandler
    public void onWorldUnload(WorldUnloadEvent event) {
        // Keep the last identity. An already-running async edit may finish while
        // the Bukkit world is transitioning out of the loaded-world registry.
        remember(event.getWorld());
    }

    private void remember(World world) {
        worldIds.put(normalize(world.getName()), world.getUID());
    }

    private static String normalize(String value) {
        return value.toLowerCase(Locale.ROOT);
    }
}
