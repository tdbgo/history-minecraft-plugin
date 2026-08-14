package kr.playcity.history.integration.worldedit;

import com.sk89q.worldedit.EditSession;
import com.sk89q.worldedit.WorldEdit;
import com.sk89q.worldedit.event.extent.EditSessionEvent;
import com.sk89q.worldedit.extension.platform.Actor;
import com.sk89q.worldedit.util.eventbus.Subscribe;
import kr.playcity.history.capture.ChangeRecorder;
import kr.playcity.history.model.ActorRef;

import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class WorldEditIntegration implements AutoCloseable {
    private static final long UNKNOWN_WORLD_WARNING_INTERVAL_NANOS = 30_000_000_000L;

    private final ChangeRecorder recorder;
    private final WorldIdentityIndex worlds;
    private final Logger logger;
    private final boolean faweAvailable;
    private final AtomicBoolean registered = new AtomicBoolean();
    private final AtomicBoolean faweBridgeFailureLogged = new AtomicBoolean();
    private final AtomicLong lastUnknownWorldWarning = new AtomicLong();

    public WorldEditIntegration(
        ChangeRecorder recorder,
        WorldIdentityIndex worlds,
        Logger logger,
        boolean faweAvailable
    ) {
        this.recorder = Objects.requireNonNull(recorder, "recorder");
        this.worlds = Objects.requireNonNull(worlds, "worlds");
        this.logger = Objects.requireNonNull(logger, "logger");
        this.faweAvailable = faweAvailable;
    }

    public void register() {
        if (registered.compareAndSet(false, true)) {
            WorldEdit.getInstance().getEventBus().register(this);
        }
    }

    @Subscribe
    public void onEditSession(EditSessionEvent event) {
        if (!registered.get()
            || event.getStage() != EditSession.Stage.BEFORE_CHANGE
            || event.getWorld() == null) {
            return;
        }
        worlds.find(event.getWorld().getName()).ifPresentOrElse(
            worldId -> attach(event, worldId),
            () -> warnUnknownWorld(event.getWorld().getName())
        );
    }

    @Override
    public void close() {
        if (registered.compareAndSet(true, false)) {
            WorldEdit.getInstance().getEventBus().unregister(this);
        }
    }

    private static ActorRef actor(Actor actor) {
        if (actor == null) {
            return ActorRef.system("#worldedit");
        }
        String name = actor.getName() == null || actor.getName().isBlank()
            ? "#worldedit"
            : actor.getName();
        UUID uniqueId = actor.getUniqueId();
        if (actor.isPlayer() && uniqueId != null) {
            return ActorRef.player(uniqueId, name);
        }
        return ActorRef.system(name);
    }

    private void attach(EditSessionEvent event, UUID worldId) {
        ActorRef actor = actor(event.getActor());
        UUID batchId = UUID.randomUUID();
        if (faweAvailable) {
            try {
                if (FaweEditSessionBridge.attach(
                    event.getExtent(),
                    worldId,
                    actor,
                    batchId,
                    recorder,
                    logger
                )) {
                    return;
                }
            } catch (RuntimeException | LinkageError failure) {
                if (faweBridgeFailureLogged.compareAndSet(false, true)) {
                    logger.log(
                        Level.SEVERE,
                        "History could not attach its FAWE batch processor; using the WorldEdit extent fallback",
                        failure
                    );
                }
            }
        }
        event.setExtent(new WorldEditChangeExtent(
            event.getExtent(),
            worldId,
            actor,
            batchId,
            recorder
        ));
    }

    private void warnUnknownWorld(String worldName) {
        long now = System.nanoTime();
        long previous = lastUnknownWorldWarning.get();
        if (now - previous >= UNKNOWN_WORLD_WARNING_INTERVAL_NANOS
            && lastUnknownWorldWarning.compareAndSet(previous, now)) {
            logger.warning("WorldEdit change was not recorded because the Bukkit world identity is unknown: " + worldName);
        }
    }
}
