package kr.playcity.history;

import kr.playcity.history.capture.ActorResolver;
import kr.playcity.history.capture.ActivityListener;
import kr.playcity.history.capture.BlockChangeListener;
import kr.playcity.history.capture.ChangeRecorder;
import kr.playcity.history.capture.ContainerChangeListener;
import kr.playcity.history.capture.ExtendedBlockChangeListener;
import kr.playcity.history.capture.SnapshotCodec;
import kr.playcity.history.command.HistoryCommand;
import kr.playcity.history.command.InspectionService;
import kr.playcity.history.config.ConfigException;
import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.integration.worldedit.WorldEditIntegration;
import kr.playcity.history.integration.worldedit.WorldIdentityIndex;
import kr.playcity.history.rollback.PreviewRegistry;
import kr.playcity.history.rollback.ActivePositionGuard;
import kr.playcity.history.rollback.RollbackPlanner;
import kr.playcity.history.rollback.RollbackService;
import kr.playcity.history.storage.AsyncHistoryStore;
import org.bukkit.command.PluginCommand;
import org.bukkit.plugin.java.JavaPlugin;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;

public final class HistoryPlugin extends JavaPlugin {
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 60L;
    private AsyncHistoryStore store;
    private RollbackService rollbackService;
    private AutoCloseable worldEditIntegration;

    @Override
    public void onEnable() {
        saveDefaultConfig();

        HistoryConfig historyConfig;
        try {
            historyConfig = HistoryConfig.load(getConfig(), getDataFolder().toPath());
        } catch (ConfigException exception) {
            getLogger().log(Level.SEVERE, "History configuration is invalid: " + exception.getMessage());
            getServer().getPluginManager().disablePlugin(this);
            return;
        }

        store = new AsyncHistoryStore(historyConfig.storage(), getLogger());
        SnapshotCodec snapshots = new SnapshotCodec();
        ActorResolver actors = new ActorResolver();
        ActivePositionGuard positionGuard = new ActivePositionGuard();
        ChangeRecorder recorder = new ChangeRecorder(store, getLogger(), positionGuard);
        worldEditIntegration = enableWorldEditIntegration(historyConfig, recorder);
        InspectionService inspection = new InspectionService(this, store, historyConfig.inspection());
        rollbackService = new RollbackService(
            this,
            historyConfig.rollback(),
            store,
            snapshots,
            new RollbackPlanner(),
            new PreviewRegistry(),
            positionGuard
        );

        getServer().getPluginManager().registerEvents(
            new BlockChangeListener(this, historyConfig.logging(), snapshots, actors, recorder),
            this
        );
        getServer().getPluginManager().registerEvents(
            new ExtendedBlockChangeListener(this, historyConfig.logging(), snapshots, actors, recorder),
            this
        );
        getServer().getPluginManager().registerEvents(
            new ContainerChangeListener(this, historyConfig.logging(), snapshots, actors, recorder),
            this
        );
        getServer().getPluginManager().registerEvents(
            new ActivityListener(this, historyConfig.logging(), actors, recorder),
            this
        );
        getServer().getPluginManager().registerEvents(inspection, this);

        PluginCommand historyCommand = getCommand("history");
        if (historyCommand == null) {
            getLogger().severe("plugin.yml did not register the history command");
            getServer().getPluginManager().disablePlugin(this);
            return;
        }
        HistoryCommand commandHandler = new HistoryCommand(
            this,
            historyConfig,
            store,
            inspection,
            rollbackService
        );
        historyCommand.setExecutor(commandHandler);
        historyCommand.setTabCompleter(commandHandler);

        getLogger().info(
            "History " + getPluginMeta().getVersion()
                + " enabled for Paper API " + getPluginMeta().getAPIVersion()
                + "; " + historyConfig.storage().backend().displayName()
                + " storage is warming asynchronously."
        );
    }

    @Override
    public void onDisable() {
        closeWorldEditIntegration();
        if (store == null) {
            return;
        }
        java.util.concurrent.CompletableFuture<Void> shutdown;
        if (rollbackService == null) {
            shutdown = store.closeAsync();
        } else {
            shutdown = rollbackService.shutdown().handle((unused, failure) -> {
                if (failure != null) {
                    getLogger().log(Level.SEVERE, "Unable to finalize an active History operation", failure);
                }
                return null;
            }).thenCompose(unused -> store.closeAsync());
        }
        try {
            shutdown.get(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            getLogger().log(Level.SEVERE, "History shutdown was interrupted before storage closed", interrupted);
        } catch (ExecutionException failure) {
            getLogger().log(Level.SEVERE, "History could not flush and close its storage cleanly", failure.getCause());
        } catch (TimeoutException timeout) {
            getLogger().log(
                Level.SEVERE,
                "History storage did not close within " + SHUTDOWN_TIMEOUT_SECONDS
                    + " seconds; inspect /history status and the preceding storage error",
                timeout
            );
        }
    }

    private AutoCloseable enableWorldEditIntegration(HistoryConfig config, ChangeRecorder recorder) {
        if (!config.logging().worldEdit()) {
            getLogger().info("WorldEdit/FAWE history capture is disabled by configuration.");
            return null;
        }
        boolean faweAvailable = getServer().getPluginManager().isPluginEnabled("FastAsyncWorldEdit");
        boolean available = getServer().getPluginManager().isPluginEnabled("WorldEdit") || faweAvailable;
        if (!available) {
            getLogger().info("WorldEdit/FAWE was not found; continuing without edit-session capture.");
            return null;
        }
        try {
            WorldIdentityIndex worlds = new WorldIdentityIndex();
            getServer().getPluginManager().registerEvents(worlds, this);
            WorldEditIntegration integration = new WorldEditIntegration(
                recorder,
                worlds,
                getLogger(),
                faweAvailable
            );
            integration.register();
            getLogger().info("WorldEdit/FAWE edit-session capture enabled through public integration APIs.");
            return integration;
        } catch (LinkageError | RuntimeException failure) {
            getLogger().log(Level.SEVERE, "WorldEdit/FAWE integration could not be enabled", failure);
            return null;
        }
    }

    private void closeWorldEditIntegration() {
        if (worldEditIntegration == null) {
            return;
        }
        try {
            worldEditIntegration.close();
        } catch (Exception | LinkageError failure) {
            getLogger().log(Level.WARNING, "Unable to detach the WorldEdit/FAWE integration cleanly", failure);
        } finally {
            worldEditIntegration = null;
        }
    }
}
