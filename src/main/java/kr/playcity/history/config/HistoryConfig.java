package kr.playcity.history.config;

import kr.playcity.history.util.DurationParser;
import org.bukkit.configuration.file.FileConfiguration;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;

public record HistoryConfig(
    Storage storage,
    Logging logging,
    Inspection inspection,
    Rollback rollback
) {
    public HistoryConfig {
        storage = Objects.requireNonNull(storage, "storage");
        logging = Objects.requireNonNull(logging, "logging");
        inspection = Objects.requireNonNull(inspection, "inspection");
        rollback = Objects.requireNonNull(rollback, "rollback");
    }

    public static HistoryConfig load(FileConfiguration source, Path dataDirectory) {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(dataDirectory, "dataDirectory");

        if (source.getInt("config-version", -1) != 1) {
            throw new ConfigException("config-version must be 1");
        }

        StorageBackend backend = StorageBackend.parse(source.getString("storage.backend", "sqlite"));
        String configuredFile = source.getString("storage.sqlite.file",
            source.getString("storage.file", "history.db"));
        if (configuredFile == null || configuredFile.isBlank()) {
            throw new ConfigException("storage.sqlite.file must not be blank");
        }
        Path normalizedDataDirectory = dataDirectory.toAbsolutePath().normalize();
        Path databaseFile = normalizedDataDirectory.resolve(configuredFile.trim()).normalize();
        if (!databaseFile.startsWith(normalizedDataDirectory)) {
            throw new ConfigException("storage.sqlite.file must remain inside the History data directory");
        }

        Postgres postgres = loadPostgres(source, backend);
        Storage storage = new Storage(
            backend,
            databaseFile,
            postgres,
            boundedInt(source, "storage.queue-capacity", 1_000, 1_000_000),
            boundedInt(source, "storage.batch-size", 1, 10_000),
            boundedInt(source, "storage.flush-interval-ms", 10, 5_000),
            boundedIntFallback(source, "storage.sqlite.busy-timeout-ms", "storage.busy-timeout-ms", 100, 60_000),
            boundedIntDefault(source, "storage.retention.days", 0, 0, 36_500),
            boundedIntDefault(source, "storage.retention.purge-batch-size", 10_000, 100, 1_000_000),
            boundedIntDefault(source, "storage.retention.maintenance-interval-minutes", 60, 1, 1_440)
        );
        Logging logging = new Logging(
            source.getBoolean("logging.player-blocks", true),
            source.getBoolean("logging.explosions", true),
            source.getBoolean("logging.natural-changes", true),
            source.getBoolean("logging.pistons", true),
            source.getBoolean("logging.liquids", true),
            source.getBoolean("logging.worldedit", true),
            source.getBoolean("logging.player-interactions", true),
            source.getBoolean("logging.containers", true),
            source.getBoolean("logging.signs", true),
            source.getBoolean("logging.player-sessions", true),
            source.getBoolean("logging.player-commands", true),
            source.getBoolean("logging.player-messages", false),
            source.getBoolean("logging.items", true),
            source.getBoolean("logging.entities", true),
            source.getStringList("logging.redacted-command-prefixes")
        );
        Inspection inspection = new Inspection(
            boundedInt(source, "inspection.result-limit", 1, 50),
            boundedInt(source, "inspection.nearby-radius", 1, 100)
        );

        Duration defaultDuration;
        try {
            defaultDuration = DurationParser.parse(requiredString(source, "rollback.default-duration"));
        } catch (IllegalArgumentException exception) {
            throw new ConfigException("rollback.default-duration is invalid: " + exception.getMessage());
        }

        int defaultRadius = boundedInt(source, "rollback.default-radius", 1, 30_000_000);
        int maxRadius = boundedInt(source, "rollback.max-radius", 1, 30_000_000);
        if (defaultRadius > maxRadius) {
            throw new ConfigException("rollback.default-radius must not exceed rollback.max-radius");
        }
        Rollback rollback = new Rollback(
            defaultDuration,
            defaultRadius,
            maxRadius,
            boundedInt(source, "rollback.max-source-changes", 1, 1_000_000),
            boundedIntDefault(source, "rollback.max-chunks-per-operation", 256, 1, 10_000),
            boundedIntDefault(source, "rollback.max-concurrent-chunk-leases", 4, 1, 32),
            boundedIntDefault(source, "rollback.chunk-load-timeout-seconds", 30, 5, 300),
            source.getBoolean("rollback.generate-missing-chunks", true),
            boundedInt(source, "rollback.blocks-per-tick", 1, 10_000),
            boundedInt(source, "rollback.preview-ttl-seconds", 10, 3_600),
            source.getBoolean("rollback.restore-block-entity-data", false)
        );
        return new HistoryConfig(storage, logging, inspection, rollback);
    }

    private static String requiredString(FileConfiguration source, String path) {
        String value = source.getString(path);
        if (value == null || value.isBlank()) {
            throw new ConfigException(path + " must not be blank");
        }
        return value.trim();
    }

    private static int boundedInt(FileConfiguration source, String path, int minimum, int maximum) {
        int value = source.getInt(path, Integer.MIN_VALUE);
        if (value < minimum || value > maximum) {
            throw new ConfigException(path + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    private static int boundedIntDefault(
        FileConfiguration source,
        String path,
        int defaultValue,
        int minimum,
        int maximum
    ) {
        int value = source.getInt(path, defaultValue);
        if (value < minimum || value > maximum) {
            throw new ConfigException(path + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    private static int boundedIntFallback(
        FileConfiguration source,
        String path,
        String fallbackPath,
        int minimum,
        int maximum
    ) {
        int value = source.contains(path)
            ? source.getInt(path, Integer.MIN_VALUE)
            : source.getInt(fallbackPath, Integer.MIN_VALUE);
        if (value < minimum || value > maximum) {
            throw new ConfigException(path + " must be between " + minimum + " and " + maximum);
        }
        return value;
    }

    private static Postgres loadPostgres(FileConfiguration source, StorageBackend backend) {
        String prefix = "storage.postgresql.";
        String host = source.getString(prefix + "host", "127.0.0.1").trim();
        String database = source.getString(prefix + "database", "history").trim();
        String schema = source.getString(prefix + "schema", "history").trim();
        String username = source.getString(prefix + "username", "history").trim();
        String sslMode = source.getString(prefix + "ssl-mode", "require").trim().toLowerCase(java.util.Locale.ROOT);
        String passwordEnvironment = source.getString(prefix + "password-env", "").trim();
        String password = source.getString(prefix + "password", "");
        if (!passwordEnvironment.isEmpty()) {
            String environmentValue = System.getenv(passwordEnvironment);
            if (environmentValue == null && backend == StorageBackend.POSTGRESQL) {
                throw new ConfigException("environment variable " + passwordEnvironment + " is not set");
            }
            password = environmentValue == null ? "" : environmentValue;
        }
        if (!host.matches("[A-Za-z0-9._:\\-]{1,255}")) {
            throw new ConfigException(prefix + "host is invalid");
        }
        if (!database.matches("[A-Za-z0-9_.\\-]{1,63}")) {
            throw new ConfigException(prefix + "database is invalid");
        }
        if (!schema.matches("[A-Za-z_][A-Za-z0-9_]{0,62}")) {
            throw new ConfigException(prefix + "schema is invalid");
        }
        if (username.isEmpty() || username.length() > 128 || username.chars().anyMatch(Character::isISOControl)) {
            throw new ConfigException(prefix + "username is invalid");
        }
        if (!java.util.Set.of("disable", "allow", "prefer", "require", "verify-ca", "verify-full").contains(sslMode)) {
            throw new ConfigException(prefix + "ssl-mode is invalid");
        }
        return new Postgres(
            host,
            boundedIntDefault(source, prefix + "port", 5432, 1, 65_535),
            database,
            schema,
            username,
            password,
            sslMode,
            boundedIntDefault(source, prefix + "connect-timeout-ms", 5_000, 100, 60_000),
            boundedIntDefault(source, prefix + "socket-timeout-seconds", 30, 1, 600)
        );
    }

    public record Storage(
        StorageBackend backend,
        Path databaseFile,
        Postgres postgresql,
        int queueCapacity,
        int batchSize,
        int flushIntervalMillis,
        int busyTimeoutMillis,
        int retentionDays,
        int purgeBatchSize,
        int maintenanceIntervalMinutes
    ) {
        public Storage {
            backend = Objects.requireNonNull(backend, "backend");
            databaseFile = Objects.requireNonNull(databaseFile, "databaseFile");
            postgresql = Objects.requireNonNull(postgresql, "postgresql");
        }

        public Storage(
            Path databaseFile,
            int queueCapacity,
            int batchSize,
            int flushIntervalMillis,
            int busyTimeoutMillis
        ) {
            this(
                StorageBackend.SQLITE,
                databaseFile,
                Postgres.defaults(),
                queueCapacity,
                batchSize,
                flushIntervalMillis,
                busyTimeoutMillis,
                0,
                10_000,
                60
            );
        }
    }

    public record Postgres(
        String host,
        int port,
        String database,
        String schema,
        String username,
        String password,
        String sslMode,
        int connectTimeoutMillis,
        int socketTimeoutSeconds
    ) {
        public Postgres {
            host = Objects.requireNonNull(host, "host");
            database = Objects.requireNonNull(database, "database");
            schema = Objects.requireNonNull(schema, "schema");
            username = Objects.requireNonNull(username, "username");
            password = Objects.requireNonNull(password, "password");
            sslMode = Objects.requireNonNull(sslMode, "sslMode");
        }

        public static Postgres defaults() {
            return new Postgres("127.0.0.1", 5432, "history", "history", "history", "", "require", 5_000, 30);
        }

        @Override
        public String toString() {
            return "Postgres[host=" + host + ", port=" + port + ", database=" + database
                + ", schema=" + schema + ", username=" + username + ", password=<redacted>, sslMode="
                + sslMode + "]";
        }
    }

    public record Logging(
        boolean playerBlocks,
        boolean explosions,
        boolean naturalChanges,
        boolean pistons,
        boolean liquids,
        boolean worldEdit,
        boolean playerInteractions,
        boolean containers,
        boolean signs,
        boolean playerSessions,
        boolean playerCommands,
        boolean playerMessages,
        boolean items,
        boolean entities,
        java.util.List<String> redactedCommandPrefixes
    ) {
        public Logging {
            redactedCommandPrefixes = redactedCommandPrefixes.stream()
                .map(value -> value.toLowerCase(java.util.Locale.ROOT).replaceFirst("^/", ""))
                .filter(value -> !value.isBlank())
                .distinct()
                .toList();
        }

        public Logging(
            boolean playerBlocks,
            boolean explosions,
            boolean naturalChanges,
            boolean pistons,
            boolean liquids,
            boolean worldEdit
        ) {
            this(
                playerBlocks,
                explosions,
                naturalChanges,
                pistons,
                liquids,
                worldEdit,
                true,
                true,
                true,
                true,
                true,
                false,
                true,
                true,
                java.util.List.of("login", "register", "changepassword", "2fa", "totp")
            );
        }
    }

    public record Inspection(int resultLimit, int nearbyRadius) {
    }

    public record Rollback(
        Duration defaultDuration,
        int defaultRadius,
        int maxRadius,
        int maxSourceChanges,
        int maxChunksPerOperation,
        int maxConcurrentChunkLeases,
        int chunkLoadTimeoutSeconds,
        boolean generateMissingChunks,
        int blocksPerTick,
        int previewTtlSeconds,
        boolean restoreBlockEntityData
    ) {
        public Rollback {
            defaultDuration = Objects.requireNonNull(defaultDuration, "defaultDuration");
        }
    }
}
