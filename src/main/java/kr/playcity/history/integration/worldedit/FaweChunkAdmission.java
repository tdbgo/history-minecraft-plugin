package kr.playcity.history.integration.worldedit;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Bounded duplicate-callback guard for post-application FAWE capture.
 *
 * <p>An earlier design kept queue permits here between pre/post callbacks. FAWE does not
 * guarantee a matching post callback, so this guard deliberately owns no storage
 * reservation and cannot leak edit capacity.</p>
 */
final class FaweChunkAdmission {
    private static final int MAXIMUM_RECENT_CHUNKS = 8_192;
    private final ReferenceQueue<Object> collectedCallbacks = new ReferenceQueue<>();
    private final Map<CallbackKey, Boolean> completed = new LinkedHashMap<>(256, 0.75f, true) {
        @Override
        protected boolean removeEldestEntry(Map.Entry<CallbackKey, Boolean> eldest) {
            return size() > MAXIMUM_RECENT_CHUNKS;
        }
    };

    synchronized boolean beginPost(long chunkKey, Object callbackIdentity) {
        CallbackKey expired;
        while ((expired = (CallbackKey) collectedCallbacks.poll()) != null) {
            completed.remove(expired);
        }
        return completed.putIfAbsent(new CallbackKey(chunkKey,
            Objects.requireNonNull(callbackIdentity, "callbackIdentity"), collectedCallbacks), Boolean.TRUE) == null;
    }

    synchronized void releaseAll() {
        completed.clear();
    }

    int pendingCount() {
        return 0;
    }

    private static final class CallbackKey extends WeakReference<Object> {
        private final long chunkKey;
        private final int identityHash;

        private CallbackKey(long chunkKey, Object callbackIdentity, ReferenceQueue<Object> queue) {
            super(callbackIdentity, queue);
            this.chunkKey = chunkKey;
            this.identityHash = System.identityHashCode(callbackIdentity);
        }

        @Override
        public boolean equals(Object value) {
            Object callback = get();
            return this == value
                || value instanceof CallbackKey other
                    && chunkKey == other.chunkKey
                    && callback != null && callback == other.get();
        }

        @Override
        public int hashCode() {
            return 31 * Long.hashCode(chunkKey) + identityHash;
        }
    }
}
