package kr.playcity.history.model;

public enum ChangeCause {
    PLAYER_PLACE(1, true),
    PLAYER_BREAK(2, true),
    EXPLOSION(3, true),
    FIRE(4, true),
    FADE(5, true),
    GROWTH(6, true),
    SPREAD(7, true),
    FORM(8, true),
    LIQUID(9, true),
    PISTON(10, true),
    ENTITY_CHANGE(11, true),
    HISTORY_ROLLBACK(12, true),
    HISTORY_UNDO(13, true),
    WORLD_EDIT(14, true),
    PLAYER_INTERACT(15, true),
    BUCKET(16, true),
    CONTAINER(17, true),
    SIGN(18, true),
    PORTAL(19, true),
    PLAYER_SESSION(20, false),
    PLAYER_COMMAND(21, false),
    PLAYER_MESSAGE(22, false),
    ITEM_DROP(23, false),
    ITEM_PICKUP(24, false),
    ENTITY_PLACE(25, false),
    ENTITY_KILL(26, false);

    private final int storageCode;
    private final boolean rollbackEligible;

    ChangeCause(int storageCode, boolean rollbackEligible) {
        this.storageCode = storageCode;
        this.rollbackEligible = rollbackEligible;
    }

    public int storageCode() {
        return storageCode;
    }

    public boolean rollbackEligible() {
        return rollbackEligible;
    }

    public static ChangeCause fromStorageCode(int code) {
        for (ChangeCause value : values()) {
            if (value.storageCode == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown change cause code: " + code);
    }
}
