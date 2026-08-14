package kr.playcity.history.storage;

import kr.playcity.history.config.HistoryConfig;

final class HistoryRepositoryFactory {
    private HistoryRepositoryFactory() {
    }

    static HistoryRepository create(HistoryConfig.Storage config) {
        return switch (config.backend()) {
            case SQLITE -> new SqliteHistoryRepository(config.databaseFile(), config.busyTimeoutMillis());
            case POSTGRESQL -> new PostgresHistoryRepository(config.postgresql());
        };
    }
}
