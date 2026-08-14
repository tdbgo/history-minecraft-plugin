package kr.playcity.history.model;

import java.util.Objects;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.UUID;

public record HistoryQuery(
    UUID worldId,
    int centerX,
    int centerZ,
    int radius,
    long since,
    String actor,
    ChangeCause cause,
    int limit,
    Integer exactX,
    Integer exactY,
    Integer exactZ,
    Set<String> includedMaterials,
    Set<String> excludedMaterials,
    boolean rollbackOnly,
    Long beforeOccurredAt,
    Long beforeId
) {
    public HistoryQuery {
        worldId = Objects.requireNonNull(worldId, "worldId");
        if (radius < 0) {
            throw new IllegalArgumentException("radius must not be negative");
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("limit must be positive");
        }
        boolean anyExact = exactX != null || exactY != null || exactZ != null;
        boolean allExact = exactX != null && exactY != null && exactZ != null;
        if (anyExact && !allExact) {
            throw new IllegalArgumentException("Exact coordinates must be all present or all absent");
        }
        if ((beforeOccurredAt == null) != (beforeId == null)) {
            throw new IllegalArgumentException("History cursor values must be both present or both absent");
        }
        actor = actor == null || actor.isBlank() || actor.equals("*") ? null : actor;
        includedMaterials = normalizeMaterials(includedMaterials);
        excludedMaterials = normalizeMaterials(excludedMaterials);
        if (!java.util.Collections.disjoint(includedMaterials, excludedMaterials)) {
            throw new IllegalArgumentException("A material cannot be both included and excluded");
        }
        if (rollbackOnly && cause != null && !cause.rollbackEligible()) {
            throw new IllegalArgumentException("An audit-only cause cannot be used in a rollback query");
        }
    }

    public static HistoryQuery nearby(
        UUID worldId,
        int centerX,
        int centerZ,
        int radius,
        long since,
        String actor,
        int limit
    ) {
        return new HistoryQuery(
            worldId,
            centerX,
            centerZ,
            radius,
            since,
            actor,
            null,
            limit,
            null,
            null,
            null,
            Set.of(),
            Set.of(),
            false,
            null,
            null
        );
    }

    public static HistoryQuery nearby(
        UUID worldId,
        int centerX,
        int centerZ,
        int radius,
        long since,
        String actor,
        ChangeCause cause,
        int limit
    ) {
        return new HistoryQuery(
            worldId,
            centerX,
            centerZ,
            radius,
            since,
            actor,
            cause,
            limit,
            null,
            null,
            null,
            Set.of(),
            Set.of(),
            false,
            null,
            null
        );
    }

    public static HistoryQuery nearby(
        UUID worldId,
        int centerX,
        int centerZ,
        int radius,
        long since,
        String actor,
        ChangeCause cause,
        Set<String> includedMaterials,
        Set<String> excludedMaterials,
        int limit
    ) {
        return new HistoryQuery(
            worldId,
            centerX,
            centerZ,
            radius,
            since,
            actor,
            cause,
            limit,
            null,
            null,
            null,
            includedMaterials,
            excludedMaterials,
            false,
            null,
            null
        );
    }

    public static HistoryQuery at(
        UUID worldId,
        int x,
        int y,
        int z,
        long since,
        int limit
    ) {
        return new HistoryQuery(
            worldId, x, z, 0, since, null, null, limit, x, y, z, Set.of(), Set.of(), false, null, null
        );
    }

    public static HistoryQuery at(
        UUID worldId,
        int x,
        int y,
        int z,
        long since,
        String actor,
        ChangeCause cause,
        Set<String> includedMaterials,
        Set<String> excludedMaterials,
        int limit
    ) {
        return new HistoryQuery(
            worldId,
            x,
            z,
            0,
            since,
            actor,
            cause,
            limit,
            x,
            y,
            z,
            includedMaterials,
            excludedMaterials,
            false,
            null,
            null
        );
    }

    public boolean exactPosition() {
        return exactX != null;
    }

    public boolean hasCursor() {
        return beforeOccurredAt != null;
    }

    public HistoryQuery before(ChangeRecord record) {
        return before(record.occurredAt(), record.id());
    }

    public HistoryQuery forRollback() {
        return new HistoryQuery(
            worldId,
            centerX,
            centerZ,
            radius,
            since,
            actor,
            cause,
            limit,
            exactX,
            exactY,
            exactZ,
            includedMaterials,
            excludedMaterials,
            true,
            beforeOccurredAt,
            beforeId
        );
    }

    public HistoryQuery before(long occurredAt, long id) {
        return new HistoryQuery(
            worldId,
            centerX,
            centerZ,
            radius,
            since,
            actor,
            cause,
            limit,
            exactX,
            exactY,
            exactZ,
            includedMaterials,
            excludedMaterials,
            rollbackOnly,
            occurredAt,
            id
        );
    }

    private static Set<String> normalizeMaterials(Set<String> values) {
        if (values == null || values.isEmpty()) {
            return Set.of();
        }
        Set<String> normalized = new LinkedHashSet<>();
        for (String value : values) {
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Material filters must not be blank");
            }
            String key = value.toLowerCase(Locale.ROOT).trim();
            if (!key.contains(":")) {
                key = "minecraft:" + key;
            }
            if (!key.matches("[a-z0-9._-]+:[a-z0-9_./-]+")) {
                throw new IllegalArgumentException("Invalid material key: " + value);
            }
            normalized.add(key);
        }
        return java.util.Collections.unmodifiableSet(normalized);
    }
}
