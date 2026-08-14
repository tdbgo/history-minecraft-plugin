package kr.playcity.history.model;

import java.util.Arrays;
import java.util.Objects;

public record BlockSnapshot(String blockData, String payloadType, byte[] payload) {
    public static final String NO_PAYLOAD = "";
    private static final BlockSnapshot AIR = new BlockSnapshot("minecraft:air", NO_PAYLOAD, new byte[0]);

    public BlockSnapshot {
        blockData = Objects.requireNonNull(blockData, "blockData");
        payloadType = Objects.requireNonNull(payloadType, "payloadType");
        payload = Objects.requireNonNull(payload, "payload").clone();
        if (blockData.isBlank()) {
            throw new IllegalArgumentException("Block data must not be blank");
        }
        if (payloadType.isBlank() && payload.length != 0) {
            throw new IllegalArgumentException("Payload bytes require a payload type");
        }
    }

    public static BlockSnapshot block(String blockData) {
        return new BlockSnapshot(blockData, NO_PAYLOAD, new byte[0]);
    }

    public static BlockSnapshot air() {
        return AIR;
    }

    @Override
    public byte[] payload() {
        return payload.clone();
    }

    public BlockSnapshot withoutPayload() {
        if (payload.length == 0) {
            return this;
        }
        return block(blockData);
    }

    public boolean hasPayload() {
        return !payloadType.isEmpty();
    }

    public int payloadSize() {
        return payload.length;
    }

    public boolean sameState(BlockSnapshot other, boolean includePayload) {
        if (!blockData.equals(other.blockData)) {
            return false;
        }
        return !includePayload
            || payloadType.equals(other.payloadType) && Arrays.equals(payload, other.payload);
    }

    public String materialKey() {
        int stateStart = blockData.indexOf('[');
        return stateStart < 0 ? blockData : blockData.substring(0, stateStart);
    }

    @Override
    public boolean equals(Object value) {
        if (this == value) {
            return true;
        }
        if (!(value instanceof BlockSnapshot other)) {
            return false;
        }
        return blockData.equals(other.blockData)
            && payloadType.equals(other.payloadType)
            && Arrays.equals(payload, other.payload);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(blockData, payloadType);
        return 31 * result + Arrays.hashCode(payload);
    }
}
