package kr.playcity.history.config;

import java.util.Locale;

public enum StorageBackend {
    SQLITE,
    POSTGRESQL;

    public static StorageBackend parse(String value) {
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "sqlite" -> SQLITE;
            case "postgres", "postgresql", "pg", "pgdb" -> POSTGRESQL;
            default -> throw new ConfigException("storage.backend must be sqlite or postgresql");
        };
    }

    public String displayName() {
        return switch (this) {
            case SQLITE -> "SQLite";
            case POSTGRESQL -> "PostgreSQL";
        };
    }
}
