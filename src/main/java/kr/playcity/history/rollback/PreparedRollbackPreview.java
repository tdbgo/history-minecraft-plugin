package kr.playcity.history.rollback;

import java.nio.file.Path;
import java.util.Objects;

record PreparedRollbackPreview(RollbackPreview preview, Path planFile) {
    PreparedRollbackPreview {
        preview = Objects.requireNonNull(preview, "preview");
        planFile = Objects.requireNonNull(planFile, "planFile").toAbsolutePath().normalize();
    }
}
