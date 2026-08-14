package kr.playcity.history.command;

import kr.playcity.history.model.ChangeCause;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class HistoryCliParserTest {
    @Test
    void parsesCoreProtectStyleLookupFilters() {
        HistoryCliParser.LookupSpec spec = HistoryCliParser.parse(
            new String[] {
                "u:Builder", "t:1d12h", "r:0", "a:container", "i:chest,!barrel",
                "w:world", "x:-20", "y:64", "z:30", "limit:25"
            },
            10,
            8
        );

        assertEquals("Builder", spec.actor());
        assertEquals(Duration.ofHours(36), spec.duration());
        assertEquals(ChangeCause.CONTAINER, spec.cause());
        assertEquals(Set.of("minecraft:chest"), spec.includedMaterials());
        assertEquals(Set.of("minecraft:barrel"), spec.excludedMaterials());
        assertTrue(spec.exactPosition());
        assertEquals(25, spec.limit());
    }

    @Test
    void rejectsPartialOrAmbiguousCoordinates() {
        assertThrows(
            IllegalArgumentException.class,
            () -> HistoryCliParser.parse(new String[] {"x:10"}, 10, 8)
        );
        assertThrows(
            IllegalArgumentException.class,
            () -> HistoryCliParser.parse(new String[] {"x:10", "y:64", "z:10", "r:5"}, 10, 8)
        );
    }
}
