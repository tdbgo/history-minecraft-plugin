package kr.playcity.history.capture;

import kr.playcity.history.model.ActorRef;
import org.bukkit.entity.Entity;
import org.bukkit.entity.Player;
import org.bukkit.entity.Projectile;
import org.bukkit.entity.TNTPrimed;
import org.bukkit.projectiles.ProjectileSource;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

public final class ActorResolver {
    private static final int MAXIMUM_CAUSE_DEPTH = 8;

    public ActorRef player(Player player) {
        return ActorRef.player(player.getUniqueId(), player.getName());
    }

    public ActorRef entity(Entity entity) {
        Entity current = entity;
        Set<UUID> visited = new HashSet<>();
        for (int depth = 0; depth < MAXIMUM_CAUSE_DEPTH && visited.add(current.getUniqueId()); depth++) {
            if (current instanceof Player player) {
                return player(player);
            }
            Entity source = sourceOf(current);
            if (source == null) {
                break;
            }
            current = source;
        }
        String entityName = "#" + entity.getType().getKey().getKey().toLowerCase(Locale.ROOT);
        return ActorRef.entity(entityName);
    }

    private static Entity sourceOf(Entity entity) {
        if (entity instanceof TNTPrimed tnt && tnt.getSource() != null) {
            return tnt.getSource();
        }
        if (entity instanceof Projectile projectile) {
            ProjectileSource shooter = projectile.getShooter();
            if (shooter instanceof Entity sourceEntity) {
                return sourceEntity;
            }
        }
        return null;
    }
}
