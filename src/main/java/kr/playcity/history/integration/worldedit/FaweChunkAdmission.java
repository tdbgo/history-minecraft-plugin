package kr.playcity.history.integration.worldedit;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded duplicate-callback guard for post-application FAWE capture.
 *
 * <p>alpha.5 kept queue permits here between pre/post callbacks. FAWE does not
 * guarantee a matching post callback, so alpha.6 deliberately owns no storage
 * reservation and cannot leak edit capacity.</p>
 */
final class FaweChunkAdmission {
    private static final int MAXIMUM_RECENT_CHUNKS = 8_192;
    private final Map<Long, Boolean> completed = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<Long, Boolean> eldest) {
            return size() > MAXIMUM_RECENT_CHUNKS;
        }
    };

    synchronized boolean beginPost(long chunkKey) {
        return completed.putIfAbsent(chunkKey, Boolean.TRUE) == null;
    }

    synchronized void releaseAll() {
        completed.clear();
    }

    int pendingCount() {
        return 0;
    }
}
