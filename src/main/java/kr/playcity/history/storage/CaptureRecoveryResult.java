package kr.playcity.history.storage;

import java.util.Objects;

public record CaptureRecoveryResult(boolean resumed, String message) {
    public CaptureRecoveryResult {
        message = Objects.requireNonNull(message, "message");
    }
}
