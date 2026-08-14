package kr.playcity.history.rollback;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public final class PreviewRegistry {
    private static final char[] TOKEN_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz".toCharArray();
    private static final int TOKEN_LENGTH = 10;
    private final SecureRandom random = new SecureRandom();
    private final Map<String, RollbackPreview> previews = new ConcurrentHashMap<>();

    public RollbackPreview register(RollbackPreview preview) {
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
            preview.items(),
            preview.sourceChanges(),
            preview.conflicts(),
            preview.alreadyTarget(),
            preview.sourceLimitReached()
        );
        previews.put(token, registered);
        return registered;
    }

    public Optional<RollbackPreview> consume(String token, UUID ownerId) {
        RollbackPreview preview = previews.remove(token);
        if (preview == null || !preview.ownerId().equals(ownerId) || preview.expiresAt() < System.currentTimeMillis()) {
            return Optional.empty();
        }
        return Optional.of(preview);
    }

    public boolean cancel(String token, UUID ownerId) {
        RollbackPreview preview = previews.get(token);
        return preview != null && preview.ownerId().equals(ownerId) && previews.remove(token, preview);
    }

    private void purgeExpired() {
        long now = System.currentTimeMillis();
        previews.entrySet().removeIf(entry -> entry.getValue().expiresAt() < now);
    }

    private String newToken() {
        StringBuilder token = new StringBuilder(TOKEN_LENGTH);
        for (int index = 0; index < TOKEN_LENGTH; index++) {
            token.append(TOKEN_ALPHABET[random.nextInt(TOKEN_ALPHABET.length)]);
        }
        return token.toString();
    }
}
