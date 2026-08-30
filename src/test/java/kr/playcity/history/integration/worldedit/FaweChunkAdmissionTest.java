package kr.playcity.history.integration.worldedit;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FaweChunkAdmissionTest {
    @Test
    void duplicatePostCallbackIsIdempotentAndOwnsNoReservation() {
        FaweChunkAdmission admission = new FaweChunkAdmission();
        Object callback = new Object();

        assertTrue(admission.beginPost(10L, callback));
        assertFalse(admission.beginPost(10L, callback));
        assertEquals(0, admission.pendingCount());
    }

    @Test
    void distinctAppliedSetsForTheSameChunkAreNotDiscardedAsDuplicates() {
        FaweChunkAdmission admission = new FaweChunkAdmission();

        assertTrue(admission.beginPost(10L, new Object()));
        assertTrue(admission.beginPost(10L, new Object()));
        assertEquals(0, admission.pendingCount());
    }

    @Test
    void missingPostCallbackCannotLeakCapacity() {
        FaweChunkAdmission admission = new FaweChunkAdmission();

        // processSet does not register anything, so cancellation,
        // delegate=false, exceptions and a missing post callback all leave zero.
        assertEquals(0, admission.pendingCount());
        admission.releaseAll();
        assertEquals(0, admission.pendingCount());
    }

    @Test
    void parallelHundredsOfChunksRemainReservationFreeAndDeduplicateExactlyOnce() throws Exception {
        FaweChunkAdmission admission = new FaweChunkAdmission();
        int chunks = 256;
        CountDownLatch start = new CountDownLatch(1);
        List<Long> admitted = Collections.synchronizedList(new ArrayList<>());
        List<Thread> threads = new ArrayList<>();
        for (long chunk = 0; chunk < chunks; chunk++) {
            long key = chunk;
            Object callback = new Object();
            Thread first = Thread.ofPlatform().unstarted(() -> {
                await(start);
                if (admission.beginPost(key, callback)) {
                    admitted.add(key);
                }
            });
            Thread duplicate = Thread.ofPlatform().unstarted(() -> {
                await(start);
                if (admission.beginPost(key, callback)) {
                    admitted.add(key);
                }
            });
            threads.add(first);
            threads.add(duplicate);
            first.start();
            duplicate.start();
        }
        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(chunks, admitted.size());
        assertEquals(chunks, admitted.stream().distinct().count());
        assertEquals(0, admission.pendingCount());
    }

    @Test
    void flushOnlyClearsBoundedDeduplicationState() {
        FaweChunkAdmission admission = new FaweChunkAdmission();
        Object callback = new Object();
        assertTrue(admission.beginPost(12L, callback));

        admission.releaseAll();

        assertTrue(admission.beginPost(12L, callback));
        assertEquals(0, admission.pendingCount());
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new AssertionError(interrupted);
        }
    }
}
