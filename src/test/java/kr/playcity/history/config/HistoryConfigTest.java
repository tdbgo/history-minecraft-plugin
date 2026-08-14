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

class HistoryConfigTest {
    @TempDir
    Path dataDirectory;

    @Test
    void loadsBoundedDistantChunkDefaults() {
        HistoryConfig.Rollback rollback = HistoryConfig.load(loadDefaultConfiguration(), dataDirectory).rollback();

        assertEquals(10_000, rollback.maxRadius());
        assertEquals(256, rollback.maxChunksPerOperation());
        assertEquals(4, rollback.maxConcurrentChunkLeases());
        assertEquals(30, rollback.chunkLoadTimeoutSeconds());
        assertTrue(rollback.generateMissingChunks());
    }

    @Test
    void oldConfigurationReceivesBackwardCompatibleChunkLoadingDefaults() {
        YamlConfiguration source = loadDefaultConfiguration();
        source.set("rollback.max-concurrent-chunk-leases", null);
        source.set("rollback.chunk-load-timeout-seconds", null);
        source.set("rollback.generate-missing-chunks", null);

        HistoryConfig.Rollback rollback = HistoryConfig.load(source, dataDirectory).rollback();

        assertEquals(4, rollback.maxConcurrentChunkLeases());
        assertEquals(30, rollback.chunkLoadTimeoutSeconds());
        assertTrue(rollback.generateMissingChunks());
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
