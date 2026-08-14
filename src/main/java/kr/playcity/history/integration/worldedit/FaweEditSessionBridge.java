package kr.playcity.history.integration.worldedit;

import com.fastasyncworldedit.core.queue.IBatchProcessor;
import com.fastasyncworldedit.core.queue.IQueueExtent;
import com.fastasyncworldedit.core.util.ExtentTraverser;
import com.sk89q.worldedit.extent.Extent;
import kr.playcity.history.capture.ChangeRecorder;
import kr.playcity.history.model.ActorRef;

import java.util.UUID;
import java.util.logging.Logger;

final class FaweEditSessionBridge {
    private FaweEditSessionBridge() {
    }

    static boolean attach(
        Extent extent,
        UUID worldId,
        ActorRef actor,
        UUID batchId,
        ChangeRecorder recorder,
        Logger logger
    ) {
        IQueueExtent<?> queue = new ExtentTraverser<>(extent).findAndGet(IQueueExtent.class);
        if (queue == null) {
            return false;
        }
        IBatchProcessor history = new FaweBatchProcessor(
            worldId,
            actor,
            batchId,
            recorder,
            logger
        );
        IBatchProcessor existing = queue.getProcessor();
        queue.setProcessor(existing == null ? history : existing.join(history));
        return true;
    }
}
