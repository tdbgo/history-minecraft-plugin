package kr.playcity.history.capture;

import kr.playcity.history.model.BlockSnapshot;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.gson.GsonComponentSerializer;
import org.bukkit.Bukkit;
import org.bukkit.DyeColor;
import org.bukkit.block.Block;
import org.bukkit.block.BlockState;
import org.bukkit.block.Chest;
import org.bukkit.block.Sign;
import org.bukkit.block.TileState;
import org.bukkit.block.sign.Side;
import org.bukkit.block.sign.SignSide;
import org.bukkit.inventory.Inventory;
import org.bukkit.inventory.InventoryHolder;
import org.bukkit.inventory.ItemStack;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public final class SnapshotCodec {
    static final String INVENTORY_V1 = "inventory/v1";
    static final String SIGN_V1 = "sign/v1";
    static final String UNSUPPORTED_TILE_V1 = "unsupported-tile/v1";

    public BlockSnapshot capture(Block block) {
        return capture(block.getState(false));
    }

    public BlockSnapshot capture(BlockState state) {
        String blockData = state.getBlockData().getAsString(false);
        try {
            if (state instanceof Sign sign) {
                return new BlockSnapshot(blockData, SIGN_V1, encodeSign(sign));
            }
            Inventory inventory = inventoryOf(state);
            if (inventory != null) {
                return new BlockSnapshot(
                    blockData,
                    INVENTORY_V1,
                    ItemStack.serializeItemsAsBytes(inventory.getContents())
                );
            }
            if (state instanceof TileState) {
                // Preserve an explicit boundary so rollback planning never
                // mistakes an uncaptured block entity for an ordinary block.
                return new BlockSnapshot(blockData, UNSUPPORTED_TILE_V1, new byte[0]);
            }
            return BlockSnapshot.block(blockData);
        } catch (RuntimeException | IOException exception) {
            throw new SnapshotException("Unable to capture block-entity data at " + state.getLocation(), exception);
        }
    }

    public void apply(Block block, BlockSnapshot snapshot, boolean restorePayload) {
        block.setBlockData(Bukkit.createBlockData(snapshot.blockData()), false);
        if (!restorePayload || snapshot.payloadType().isEmpty()) {
            return;
        }
        BlockState state = block.getState(false);
        try {
            switch (snapshot.payloadType()) {
                case INVENTORY_V1 -> restoreInventory(state, snapshot.payload());
                case SIGN_V1 -> restoreSign(state, snapshot.payload());
                default -> throw new SnapshotException(
                    "Unsupported block payload type: " + snapshot.payloadType()
                );
            }
        } catch (IOException | RuntimeException exception) {
            if (exception instanceof SnapshotException snapshotException) {
                throw snapshotException;
            }
            throw new SnapshotException("Unable to restore block-entity data at " + block.getLocation(), exception);
        }
    }

    private static Inventory inventoryOf(BlockState state) {
        if (state instanceof Chest chest) {
            return chest.getBlockInventory();
        }
        if (state instanceof InventoryHolder holder) {
            return holder.getInventory();
        }
        return null;
    }

    private static void restoreInventory(BlockState state, byte[] payload) {
        Inventory inventory = inventoryOf(state);
        if (inventory == null) {
            throw new SnapshotException("Stored inventory payload no longer targets an inventory block");
        }
        ItemStack[] items = ItemStack.deserializeItemsFromBytes(payload);
        if (items.length > inventory.getSize()) {
            throw new SnapshotException("Stored inventory is larger than the target inventory");
        }
        inventory.clear();
        inventory.setContents(items);
        state.update(true, false);
    }

    private static byte[] encodeSign(Sign sign) throws IOException {
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
             DataOutputStream output = new DataOutputStream(bytes)) {
            output.writeInt(1);
            output.writeBoolean(sign.isWaxed());
            writeSignSide(output, sign.getSide(Side.FRONT));
            writeSignSide(output, sign.getSide(Side.BACK));
            output.flush();
            return bytes.toByteArray();
        }
    }

    private static void writeSignSide(DataOutputStream output, SignSide side) throws IOException {
        output.writeUTF(side.getColor().name());
        output.writeBoolean(side.isGlowingText());
        for (int line = 0; line < 4; line++) {
            output.writeUTF(GsonComponentSerializer.gson().serialize(side.line(line)));
        }
    }

    private static void restoreSign(BlockState state, byte[] payload) throws IOException {
        if (!(state instanceof Sign sign)) {
            throw new SnapshotException("Stored sign payload no longer targets a sign block");
        }
        try (DataInputStream input = new DataInputStream(new ByteArrayInputStream(payload))) {
            int version = input.readInt();
            if (version != 1) {
                throw new SnapshotException("Unsupported sign payload version: " + version);
            }
            sign.setWaxed(input.readBoolean());
            readSignSide(input, sign.getSide(Side.FRONT));
            readSignSide(input, sign.getSide(Side.BACK));
            if (input.available() != 0) {
                throw new SnapshotException("Sign payload contains trailing data");
            }
            sign.update(true, false);
        }
    }

    private static void readSignSide(DataInputStream input, SignSide side) throws IOException {
        DyeColor color;
        try {
            color = DyeColor.valueOf(input.readUTF());
        } catch (IllegalArgumentException exception) {
            throw new SnapshotException("Sign payload contains an invalid text color", exception);
        }
        side.setColor(color);
        side.setGlowingText(input.readBoolean());
        for (int line = 0; line < 4; line++) {
            Component component = GsonComponentSerializer.gson().deserialize(input.readUTF());
            side.line(line, component);
        }
    }
}
