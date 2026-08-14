package kr.playcity.history.util;

import java.time.Duration;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class DurationParser {
    private static final Pattern PART = Pattern.compile("(\\d+)([smhdw])", Pattern.CASE_INSENSITIVE);
    private static final Duration MAXIMUM = Duration.ofDays(3650);

    private DurationParser() {
    }

    public static Duration parse(String input) {
        if (input == null || input.isBlank()) {
            throw new IllegalArgumentException("duration must not be blank");
        }
        String normalized = input.trim().toLowerCase(Locale.ROOT).replace(" ", "");
        Matcher matcher = PART.matcher(normalized);
        Duration total = Duration.ZERO;
        int end = 0;
        while (matcher.find()) {
            if (matcher.start() != end) {
                throw new IllegalArgumentException("expected values such as 15m, 2h, or 1d12h");
            }
            long amount;
            try {
                amount = Long.parseLong(matcher.group(1));
            } catch (NumberFormatException exception) {
                throw new IllegalArgumentException("duration number is too large", exception);
            }
            Duration part = switch (matcher.group(2).charAt(0)) {
                case 's' -> Duration.ofSeconds(amount);
                case 'm' -> Duration.ofMinutes(amount);
                case 'h' -> Duration.ofHours(amount);
                case 'd' -> Duration.ofDays(amount);
                case 'w' -> Duration.ofDays(Math.multiplyExact(amount, 7L));
                default -> throw new IllegalArgumentException("unsupported duration unit");
            };
            try {
                total = total.plus(part);
            } catch (ArithmeticException exception) {
                throw new IllegalArgumentException("duration is too large", exception);
            }
            end = matcher.end();
        }
        if (end != normalized.length() || total.isZero() || total.isNegative()) {
            throw new IllegalArgumentException("expected values such as 15m, 2h, or 1d12h");
        }
        if (total.compareTo(MAXIMUM) > 0) {
            throw new IllegalArgumentException("duration must not exceed 10 years");
        }
        return total;
    }

    public static String compact(Duration duration) {
        long seconds = duration.toSeconds();
        if (seconds % 86_400L == 0L) {
            return seconds / 86_400L + "d";
        }
        if (seconds % 3_600L == 0L) {
            return seconds / 3_600L + "h";
        }
        if (seconds % 60L == 0L) {
            return seconds / 60L + "m";
        }
        return seconds + "s";
    }
}
