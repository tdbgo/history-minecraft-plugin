package kr.playcity.history.model;

public enum OperationKind {
    ROLLBACK(1),
    UNDO(2);

    private final int storageCode;

    OperationKind(int storageCode) {
        this.storageCode = storageCode;
    }

    public int storageCode() {
        return storageCode;
    }

    public static OperationKind fromStorageCode(int code) {
        for (OperationKind value : values()) {
            if (value.storageCode == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown operation kind code: " + code);
    }
}
