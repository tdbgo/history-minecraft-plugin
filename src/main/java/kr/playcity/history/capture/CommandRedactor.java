package kr.playcity.history.capture;

import java.util.List;
import java.util.Locale;

final class CommandRedactor {
    private CommandRedactor() {
    }

    static String redact(String input, List<String> redactedPrefixes) {
        String command = input.startsWith("/") ? input.substring(1) : input;
        String normalized = command.toLowerCase(Locale.ROOT);
        for (String prefix : redactedPrefixes) {
            if (normalized.equals(prefix) || normalized.startsWith(prefix + " ")) {
                return "/" + prefix + " <redacted>";
            }
        }
        return "/" + command;
    }
}
