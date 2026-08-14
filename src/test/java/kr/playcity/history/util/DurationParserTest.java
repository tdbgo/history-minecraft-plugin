package kr.playcity.history.util;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DurationParserTest {
    @Test
    void parsesCompositeDurations() {
        assertEquals(Duration.ofHours(36).plusMinutes(30), DurationParser.parse("1d12h30m"));
        assertEquals(Duration.ofMinutes(15), DurationParser.parse("15M"));
    }

    @Test
    void rejectsAmbiguousOrUnsafeValues() {
        assertThrows(IllegalArgumentException.class, () -> DurationParser.parse("15"));
        assertThrows(IllegalArgumentException.class, () -> DurationParser.parse("0m"));
        assertThrows(IllegalArgumentException.class, () -> DurationParser.parse("1h!"));
        assertThrows(IllegalArgumentException.class, () -> DurationParser.parse("5201w"));
    }

    @Test
    void formatsSingleUnitDurations() {
        assertEquals("15m", DurationParser.compact(Duration.ofMinutes(15)));
        assertEquals("2h", DurationParser.compact(Duration.ofHours(2)));
        assertEquals("2d", DurationParser.compact(Duration.ofDays(2)));
    }
}
