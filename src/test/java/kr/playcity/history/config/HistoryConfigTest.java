package kr.playcity.history.config;

import org.bukkit.configuration.file.YamlConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class HistoryConfigTest {
    @TempDir
    Path dataDirectory;

    @Test
    void loadsBoundedDistantChunkDefaults() {
        HistoryConfig config = HistoryConfig.load(loadDefaultConfiguration(), dataDirectory);
        HistoryConfig.Rollback rollback = config.rollback();

        assertEquals(131_072, config.storage().worldEditQueueCapacity());
        assertEquals(30_000, config.storage().worldEditAdmissionTimeoutMillis());
        assertEquals(10_000, rollback.maxRadius());
        assertEquals(256, rollback.maxChunksPerOperation());
        assertEquals(4, rollback.maxConcurrentChunkLeases());
        assertEquals(30, rollback.chunkLoadTimeoutSeconds());
        assertTrue(rollback.generateMissingChunks());
    }

    @Test
    void oldConfigurationReceivesBackwardCompatibleChunkLoadingDefaults() {
        YamlConfiguration source = loadDefaultConfiguration();
        source.set("storage.worldedit-queue-capacity", null);
        source.set("storage.worldedit-admission-timeout-ms", null);
        source.set("rollback.max-concurrent-chunk-leases", null);
        source.set("rollback.chunk-load-timeout-seconds", null);
        source.set("rollback.generate-missing-chunks", null);

        HistoryConfig config = HistoryConfig.load(source, dataDirectory);
        HistoryConfig.Rollback rollback = config.rollback();

        assertEquals(131_072, config.storage().worldEditQueueCapacity());
        assertEquals(30_000, config.storage().worldEditAdmissionTimeoutMillis());
        assertEquals(4, rollback.maxConcurrentChunkLeases());
        assertEquals(30, rollback.chunkLoadTimeoutSeconds());
        assertTrue(rollback.generateMissingChunks());
    }

    @Test
    void rejectsUnsafeBlockEntityRestoration() {
        YamlConfiguration source = loadDefaultConfiguration();
        source.set("rollback.restore-block-entity-data", true);

        ConfigException failure = assertThrows(
            ConfigException.class,
            () -> HistoryConfig.load(source, dataDirectory)
        );

        assertTrue(failure.getMessage().contains("safe atomic block-entity restoration"));
    }

    private static YamlConfiguration loadDefaultConfiguration() {
        InputStream resource = HistoryConfigTest.class.getResourceAsStream("/config.yml");
        if (resource == null) {
            throw new IllegalStateException("config.yml test resource is missing");
        }
        try (InputStream stream = resource;
             InputStreamReader reader = new InputStreamReader(stream, StandardCharsets.UTF_8)) {
            return YamlConfiguration.loadConfiguration(reader);
        } catch (java.io.IOException exception) {
            throw new IllegalStateException("Unable to close config.yml test resource", exception);
        }
    }
}
