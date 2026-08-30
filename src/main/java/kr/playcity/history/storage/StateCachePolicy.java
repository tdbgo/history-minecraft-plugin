package kr.playcity.history.storage;

import kr.playcity.history.model.BlockSnapshot;

/** Keeps the entry-count LRU from retaining many large, unique container payloads. */
final class StateCachePolicy {
    static final int MAXIMUM_CACHED_PAYLOAD_BYTES = 4_096;

    private StateCachePolicy() {
    }

    static boolean shouldCache(BlockSnapshot snapshot) {
        return snapshot.payloadSize() <= MAXIMUM_CACHED_PAYLOAD_BYTES;
    }
}
