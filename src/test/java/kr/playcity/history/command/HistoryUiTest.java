package kr.playcity.history.command;

import kr.playcity.history.storage.StoreStatus;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoryUiTest {
    @Test
    void haltedCaptureCannotBeDisplayedAsNormal() {
        String text = render(new StoreStatus(
            "SQLite",
            true,
            false,
            true,
            false,
            0,
            100,
            100,
            0,
            1,
            0,
            0,
            0,
            "queue overflow"
        ));

        assertTrue(text.contains("기록 중단 · SQLite"));
        assertFalse(text.contains("정상 · SQLite"));
    }

    @Test
    void recoveredCaptureKeepsCumulativeGapVisible() {
        String text = render(new StoreStatus(
            "SQLite",
            true,
            true,
            true,
            false,
            0,
            0,
            0,
            0,
            "",
            100,
            100,
            0,
            1,
            1,
            1,
            0,
            0,
            0,
            0,
            0,
            "capture resumed with a known gap"
        ));

        assertTrue(text.contains("정상 수락 · 과거 공백 있음 · SQLite"));
        assertFalse(text.contains("정상 · SQLite"));
    }

    private static String render(StoreStatus status) {
        return PlainTextComponentSerializer.plainText().serialize(HistoryUi.status(status));
    }
}
