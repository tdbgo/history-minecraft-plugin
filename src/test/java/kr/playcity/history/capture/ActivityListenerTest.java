package kr.playcity.history.capture;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ActivityListenerTest {
    @Test
    void redactsConfiguredSensitiveCommandArguments() {
        assertEquals(
            "/login <redacted>",
            CommandRedactor.redact("/LOGIN secret-password", List.of("login", "register"))
        );
        assertEquals(
            "/warp city",
            CommandRedactor.redact("/warp city", List.of("login"))
        );
    }
}
