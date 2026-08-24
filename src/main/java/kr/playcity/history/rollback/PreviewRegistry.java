package kr.playcity.history.rollback;

import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class PreviewRegistry implements AutoCloseable {
    private static final char[] TOKEN_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz".toCharArray();
    private static final int TOKEN_LENGTH = 10;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, PreparedRollbackPreview> previews = new ConcurrentHashMap<>();

    public RollbackPreview register(RollbackPreview preview, Path planFile) {
        purgeExpired();
        String token;
        do {
            token = newToken();
        } while (previews.containsKey(token));
        RollbackPreview registered = new RollbackPreview(
            token,
            preview.ownerId(),
            preview.expiresAt(),
            preview.kind(),
            preview.summary(),
            preview.inverseOf(),
            preview.itemCount(),
            preview.chunkCount(),
            preview.sourceChanges(),
            preview.conflicts(),
            preview.alreadyTarget()
        );
        previews.put(token, new PreparedRollbackPreview(registered, planFile));
        return registered;
    }

    public Optional<PreparedRollbackPreview> consume(String token, UUID ownerId) {
        PreparedRollbackPreview prepared = previews.remove(token);
        if (prepared == null) {
            return Optional.empty();
        }
        RollbackPreview preview = prepared.preview();
        if (!preview.ownerId().equals(ownerId) || preview.expiresAt() < System.currentTimeMillis()) {
            OperationPlanSpool.delete(prepared.planFile());
            return Optional.empty();
        }
        return Optional.of(prepared);
    }

    public boolean cancel(String token, UUID ownerId) {
        PreparedRollbackPreview prepared = previews.get(token);
        if (prepared == null || !prepared.preview().ownerId().equals(ownerId)
            || !previews.remove(token, prepared)) {
            return false;
        }
        OperationPlanSpool.delete(prepared.planFile());
        return true;
    }

    private void purgeExpired() {
        long now = System.currentTimeMillis();
        previews.entrySet().removeIf(entry -> {
            PreparedRollbackPreview prepared = entry.getValue();
            if (prepared.preview().expiresAt() >= now) {
                return false;
            }
            OperationPlanSpool.delete(prepared.planFile());
            return true;
        });
    }

    @Override
    public void close() {
        previews.values().forEach(prepared -> OperationPlanSpool.delete(prepared.planFile()));
        previews.clear();
    }

    private String newToken() {
        StringBuilder token = new StringBuilder(TOKEN_LENGTH);
        for (int index = 0; index < TOKEN_LENGTH; index++) {
            token.append(TOKEN_ALPHABET[random.nextInt(TOKEN_ALPHABET.length)]);
        }
        return token.toString();
    }
}
