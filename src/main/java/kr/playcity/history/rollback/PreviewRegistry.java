package kr.playcity.history.rollback;

import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public final class PreviewRegistry implements AutoCloseable {
    private static final char[] TOKEN_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz".toCharArray();
    private static final int TOKEN_LENGTH = 10;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, PreparedRollbackPreview> previews = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public RollbackPreview register(RollbackPreview preview, Path planFile) {
        if (closed.get()) {
            OperationPlanSpool.delete(planFile);
            throw new IllegalStateException("History preview registry is closed");
        }
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
        PreparedRollbackPreview prepared = new PreparedRollbackPreview(registered, planFile);
        previews.put(token, prepared);
        if (closed.get()) {
            previews.remove(token, prepared);
            OperationPlanSpool.delete(planFile);
            throw new IllegalStateException("History preview registry closed during registration");
        }
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
        closed.set(true);
        RuntimeException failure = null;
        for (PreparedRollbackPreview prepared : previews.values()) {
            try {
                OperationPlanSpool.delete(prepared.planFile());
            } catch (RuntimeException deleteFailure) {
                if (failure == null) {
                    failure = deleteFailure;
                } else {
                    failure.addSuppressed(deleteFailure);
                }
            }
        }
        previews.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private String newToken() {
        StringBuilder token = new StringBuilder(TOKEN_LENGTH);
        for (int index = 0; index < TOKEN_LENGTH; index++) {
            token.append(TOKEN_ALPHABET[random.nextInt(TOKEN_ALPHABET.length)]);
        }
        return token.toString();
    }
}
