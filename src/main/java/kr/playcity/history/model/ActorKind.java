package kr.playcity.history.model;

public enum ActorKind {
    PLAYER(1),
    ENTITY(2),
    NATURAL(3),
    SYSTEM(4);

    private final int storageCode;

    ActorKind(int storageCode) {
        this.storageCode = storageCode;
    }

    public int storageCode() {
        return storageCode;
    }

    public static ActorKind fromStorageCode(int code) {
        for (ActorKind value : values()) {
            if (value.storageCode == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown actor kind code: " + code);
    }
}
