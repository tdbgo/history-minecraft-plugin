package kr.playcity.history.model;

import java.util.Objects;
import java.util.UUID;

public record ActorRef(UUID uuid, String name, ActorKind kind) {
    public ActorRef {
        name = Objects.requireNonNull(name, "name");
        kind = Objects.requireNonNull(kind, "kind");
        if (name.isBlank()) {
            throw new IllegalArgumentException("Actor name must not be blank");
        }
        if (kind == ActorKind.PLAYER && uuid == null) {
            throw new IllegalArgumentException("Player actors require a UUID");
        }
    }

    public static ActorRef player(UUID uuid, String name) {
        return new ActorRef(uuid, name, ActorKind.PLAYER);
    }

    public static ActorRef entity(String name) {
        return new ActorRef(null, name, ActorKind.ENTITY);
    }

    public static ActorRef natural(String name) {
        return new ActorRef(null, name, ActorKind.NATURAL);
    }

    public static ActorRef system(String name) {
        return new ActorRef(null, name, ActorKind.SYSTEM);
    }
}
