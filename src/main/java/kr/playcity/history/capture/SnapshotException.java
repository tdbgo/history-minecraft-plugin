package kr.playcity.history.capture;

public final class SnapshotException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public SnapshotException(String message) {
        super(message);
    }

    public SnapshotException(String message, Throwable cause) {
        super(message, cause);
    }
}
