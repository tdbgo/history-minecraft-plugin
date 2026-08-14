package kr.playcity.history.command;

import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.util.DurationParser;

import java.time.Duration;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

final class HistoryCliParser {
    private HistoryCliParser() {
    }

    static LookupSpec parse(String[] arguments, int defaultRadius, int defaultLimit) {
        Duration duration = Duration.ofDays(1);
        int radius = defaultRadius;
        int limit = defaultLimit;
        String actor = null;
        ChangeCause cause = null;
        String world = null;
        Integer x = null;
        Integer y = null;
        Integer z = null;
        Set<String> includes = new LinkedHashSet<>();
        Set<String> excludes = new LinkedHashSet<>();
        Set<String> seen = new LinkedHashSet<>();

        for (String argument : arguments) {
            int separator = argument.indexOf(':');
            if (separator <= 0 || separator == argument.length() - 1) {
                throw new IllegalArgumentException("필터는 t:1d, r:10처럼 입력해 주세요: " + argument);
            }
            String key = canonicalKey(argument.substring(0, separator).toLowerCase(Locale.ROOT));
            String value = argument.substring(separator + 1);
            if (!key.equals("i") && !key.equals("e") && !seen.add(key)) {
                throw new IllegalArgumentException("필터를 두 번 입력했습니다: " + key);
            }
            switch (key) {
                case "u" -> actor = parseActor(value);
                case "t" -> duration = DurationParser.parse(value);
                case "r" -> radius = parseInteger(value, "반경");
                case "a" -> cause = ChangeCauseAliases.parse(value);
                case "i" -> addMaterials(value, includes, excludes, false);
                case "e" -> addMaterials(value, includes, excludes, true);
                case "w" -> world = parseWorld(value);
                case "x" -> x = parseInteger(value, "X");
                case "y" -> y = parseInteger(value, "Y");
                case "z" -> z = parseInteger(value, "Z");
                case "limit" -> limit = parseInteger(value, "limit");
                default -> throw new IllegalArgumentException("알 수 없는 조회 필터입니다: " + key);
            }
        }
        if (radius < 0) {
            throw new IllegalArgumentException("반경은 0 이상이어야 합니다.");
        }
        if (limit < 1) {
            throw new IllegalArgumentException("limit은 1 이상이어야 합니다.");
        }
        if ((x == null) != (z == null)) {
            throw new IllegalArgumentException("x와 z 좌표는 함께 입력해 주세요.");
        }
        if (y != null && x == null) {
            throw new IllegalArgumentException("y 좌표를 쓰려면 x와 z도 입력해 주세요.");
        }
        if (y != null && radius != 0) {
            throw new IllegalArgumentException("정확한 y 좌표 조회에는 r:0을 함께 입력해 주세요.");
        }
        if (!java.util.Collections.disjoint(includes, excludes)) {
            throw new IllegalArgumentException("같은 블록을 포함과 제외 필터에 동시에 넣을 수 없습니다.");
        }
        return new LookupSpec(
            duration,
            radius,
            actor,
            cause,
            Set.copyOf(includes),
            Set.copyOf(excludes),
            world,
            x,
            y,
            z,
            limit
        );
    }

    private static String canonicalKey(String key) {
        return switch (key) {
            case "u", "user" -> "u";
            case "t", "time" -> "t";
            case "r", "radius" -> "r";
            case "a", "action" -> "a";
            case "i", "item", "material" -> "i";
            case "e", "exclude" -> "e";
            case "w", "world" -> "w";
            case "x", "y", "z" -> key;
            case "limit", "l" -> "limit";
            default -> throw new IllegalArgumentException("알 수 없는 조회 필터입니다: " + key);
        };
    }

    private static String parseActor(String value) {
        if (value.equals("*") || value.equalsIgnoreCase("all")) {
            return null;
        }
        if (!value.matches("[A-Za-z0-9_#:\\-]{1,64}")) {
            throw new IllegalArgumentException("대상 이름이 올바르지 않습니다.");
        }
        return value;
    }

    private static String parseWorld(String value) {
        if (value.length() > 128 || value.chars().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("월드 이름이 올바르지 않습니다.");
        }
        return value;
    }

    private static void addMaterials(
        String input,
        Set<String> includes,
        Set<String> excludes,
        boolean excludeByDefault
    ) {
        for (String raw : input.split(",")) {
            String value = raw.trim();
            boolean excluded = excludeByDefault;
            if (value.startsWith("!") || value.startsWith("-")) {
                excluded = true;
                value = value.substring(1);
            } else if (value.startsWith("+")) {
                value = value.substring(1);
            }
            String normalized = normalizeMaterial(value);
            (excluded ? excludes : includes).add(normalized);
        }
    }

    private static String normalizeMaterial(String value) {
        String key = value.toLowerCase(Locale.ROOT);
        if (!key.contains(":")) {
            key = "minecraft:" + key;
        }
        if (!key.matches("[a-z0-9._-]+:[a-z0-9_./-]+")) {
            throw new IllegalArgumentException("블록 키가 올바르지 않습니다: " + value);
        }
        return key;
    }

    private static int parseInteger(String input, String label) {
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(label + " 값은 숫자여야 합니다.", exception);
        }
    }

    record LookupSpec(
        Duration duration,
        int radius,
        String actor,
        ChangeCause cause,
        Set<String> includedMaterials,
        Set<String> excludedMaterials,
        String world,
        Integer x,
        Integer y,
        Integer z,
        int limit
    ) {
        boolean hasCoordinates() {
            return x != null;
        }

        boolean exactPosition() {
            return x != null && y != null && radius == 0;
        }
    }
}
