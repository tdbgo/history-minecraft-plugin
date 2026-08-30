package kr.playcity.history.integration.worldedit;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Bounded duplicate-callback guard for post-application FAWE capture.
 *
 * <p>An earlier design kept queue permits here between pre/post callbacks. FAWE does not
 * guarantee a matching post callback, so this guard deliberately owns no storage
 * reservation and cannot leak edit capacity.</p>
 */
final class FaweChunkAdmission {
    private static final int MAXIMUM_RECENT_CHUNKS = 8_192;
    private final Map<CallbackKey, Boolean> completed = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<CallbackKey, Boolean> eldest) {
            return size() > MAXIMUM_RECENT_CHUNKS;
        }
    };

    synchronized boolean beginPost(long chunkKey, Object callbackIdentity) {
        return completed.putIfAbsent(new CallbackKey(chunkKey, callbackIdentity), Boolean.TRUE) == null;
    }

    synchronized void releaseAll() {
        completed.clear();
    }

    int pendingCount() {
        return 0;
    }

    private static final class CallbackKey {
        private final long chunkKey;
        private final Object callbackIdentity;

        private CallbackKey(long chunkKey, Object callbackIdentity) {
            this.chunkKey = chunkKey;
            this.callbackIdentity = callbackIdentity;
        }

        @Override
        public boolean equals(Object value) {
            return this == value
                || value instanceof CallbackKey other
                    && chunkKey == other.chunkKey
                    && callbackIdentity == other.callbackIdentity;
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(chunkKey) + System.identityHashCode(callbackIdentity);
        }
    }
}
