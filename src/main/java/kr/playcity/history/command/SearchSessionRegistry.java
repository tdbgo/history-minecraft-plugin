package kr.playcity.history.command;

import kr.playcity.history.model.HistoryQuery;

import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

final class SearchSessionRegistry {
    private static final char[] TOKEN_ALPHABET = "23456789abcdefghjkmnpqrstuvwxyz".toCharArray();
    private static final int TOKEN_LENGTH = 8;
    private static final long SESSION_TTL_MILLIS = 5 * 60 * 1_000L;

    private final SecureRandom random = new SecureRandom();
    private final Map<String, SearchSession> sessions = new ConcurrentHashMap<>();

    SearchSession register(String ownerId, HistoryQuery query, String summary, int pageSize) {
        purgeExpired();
        String token;
        do {
            token = newToken();
        } while (sessions.containsKey(token));
        SearchSession session = new SearchSession(
            token,
            ownerId,
            System.currentTimeMillis() + SESSION_TTL_MILLIS,
            query,
            summary,
            pageSize
        );
        sessions.put(token, session);
        return session;
    }

    Optional<SearchSession> find(String token, String ownerId) {
        SearchSession session = sessions.get(token);
        if (session == null
            || session.expiresAt() < System.currentTimeMillis()
            || !session.ownerId().equals(ownerId)) {
            if (session != null && session.expiresAt() < System.currentTimeMillis()) {
                sessions.remove(token, session);
            }
            return Optional.empty();
        }
        return Optional.of(session);
    }

    private void purgeExpired() {
        long now = System.currentTimeMillis();
        sessions.entrySet().removeIf(entry -> entry.getValue().expiresAt() < now);
    }

    private String newToken() {
        StringBuilder token = new StringBuilder(TOKEN_LENGTH);
        for (int index = 0; index < TOKEN_LENGTH; index++) {
            token.append(TOKEN_ALPHABET[random.nextInt(TOKEN_ALPHABET.length)]);
        }
        return token.toString();
    }

    record SearchSession(
        String token,
        String ownerId,
        long expiresAt,
        HistoryQuery query,
        String summary,
        int pageSize
    ) {
    }
}
