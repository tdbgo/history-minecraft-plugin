package kr.playcity.history.rollback;

import kr.playcity.history.model.OperationKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PreviewRegistryTest {
    @TempDir
    Path temporaryDirectory;

    @Test
    void registrationAfterShutdownFailsAndDeletesTheCompletedPlan() throws Exception {
        PreviewRegistry registry = new PreviewRegistry();
        registry.close();
        Path plan = Files.createTempFile(temporaryDirectory, "late-preview-", ".hplan");
        RollbackPreview preview = new RollbackPreview(
            "",
            UUID.randomUUID(),
            System.currentTimeMillis() + 60_000L,
            OperationKind.ROLLBACK,
            "late preview",
            null,
            1,
            1,
            1L,
            0,
            0
        );

        assertThrows(IllegalStateException.class, () -> registry.register(preview, plan));
        assertFalse(Files.exists(plan));
    }
}
