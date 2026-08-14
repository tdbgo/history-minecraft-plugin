package kr.playcity.history.command;

import kr.playcity.history.model.ChangeCause;

import java.util.Locale;

final class ChangeCauseAliases {
    private ChangeCauseAliases() {
    }

    static ChangeCause parse(String input) {
        return switch (input.toLowerCase(Locale.ROOT)) {
            case "*", "all" -> null;
            case "worldedit", "we", "fawe" -> ChangeCause.WORLD_EDIT;
            case "place", "+block" -> ChangeCause.PLAYER_PLACE;
            case "break", "-block" -> ChangeCause.PLAYER_BREAK;
            case "explosion" -> ChangeCause.EXPLOSION;
            case "fire" -> ChangeCause.FIRE;
            case "fade" -> ChangeCause.FADE;
            case "growth", "grow" -> ChangeCause.GROWTH;
            case "spread" -> ChangeCause.SPREAD;
            case "form" -> ChangeCause.FORM;
            case "liquid" -> ChangeCause.LIQUID;
            case "piston" -> ChangeCause.PISTON;
            case "entity", "entity-change" -> ChangeCause.ENTITY_CHANGE;
            case "rollback" -> ChangeCause.HISTORY_ROLLBACK;
            case "undo" -> ChangeCause.HISTORY_UNDO;
            case "interact", "interaction" -> ChangeCause.PLAYER_INTERACT;
            case "bucket" -> ChangeCause.BUCKET;
            case "container", "inventory" -> ChangeCause.CONTAINER;
            case "sign" -> ChangeCause.SIGN;
            case "portal" -> ChangeCause.PORTAL;
            case "session", "join", "quit" -> ChangeCause.PLAYER_SESSION;
            case "command" -> ChangeCause.PLAYER_COMMAND;
            case "message", "chat" -> ChangeCause.PLAYER_MESSAGE;
            case "drop" -> ChangeCause.ITEM_DROP;
            case "pickup" -> ChangeCause.ITEM_PICKUP;
            case "entity-place" -> ChangeCause.ENTITY_PLACE;
            case "entity-kill", "kill" -> ChangeCause.ENTITY_KILL;
            default -> throw new IllegalArgumentException(
                "알 수 없는 작업 필터입니다: " + input
            );
        };
    }
}
