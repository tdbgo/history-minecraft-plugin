package kr.playcity.history.command;

import kr.playcity.history.storage.StoreStatus;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoryUiTest {
    @Test
    void haltedCaptureCannotBeDisplayedAsNormal() {
        StoreStatus status = new StoreStatus(
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
        );
        String text = render(status);

        assertTrue(text.contains("기록 중단 · SQLite"));
        assertFalse(text.contains("정상 · SQLite"));
        assertTrue(status.recoveryAvailable());
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

    @Test
    void databaseFailureShowsThatCaptureIsStillDurablyJournaled() {
        String text = render(new StoreStatus(
            "PostgreSQL",
            true,
            true,
            false,
            false,
            25_000,
            12,
            8_388_608L,
            0,
            0L,
            0L,
            "",
            30_000L,
            5_000L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0L,
            0,
            "connection timeout"
        ));

        assertTrue(text.contains("DB 오류 · 내구 저널 수락 중 · PostgreSQL"));
        assertTrue(text.contains("메모리 대기 12"));
        assertTrue(text.contains("내구 저널/DB 대기 25000 / 8.00 MiB"));
    }

    private static String render(StoreStatus status) {
        return PlainTextComponentSerializer.plainText().serialize(HistoryUi.status(status));
    }
}
