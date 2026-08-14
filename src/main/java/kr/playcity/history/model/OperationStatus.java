package kr.playcity.history.model;

public enum OperationStatus {
    PREPARED(1),
    APPLIED(2),
    PARTIAL(3),
    FAILED(4),
    INTERRUPTED(5);

    private final int storageCode;

    OperationStatus(int storageCode) {
        this.storageCode = storageCode;
    }

    public int storageCode() {
        return storageCode;
    }

    public static OperationStatus fromStorageCode(int code) {
        for (OperationStatus value : values()) {
            if (value.storageCode == code) {
                return value;
            }
        }
        throw new IllegalArgumentException("Unknown operation status code: " + code);
    }
}
