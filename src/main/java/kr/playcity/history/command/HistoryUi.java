package kr.playcity.history.command;

import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.rollback.OperationRunResult;
import kr.playcity.history.rollback.RollbackPreview;
import kr.playcity.history.storage.StoreStatus;
import kr.playcity.history.storage.StorageProfile;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.TextComponent;
import net.kyori.adventure.text.event.ClickEvent;
import net.kyori.adventure.text.event.HoverEvent;
import net.kyori.adventure.text.format.NamedTextColor;
import net.kyori.adventure.text.format.TextDecoration;

import java.time.Duration;
import java.time.Instant;
import java.util.Locale;

public final class HistoryUi {
    private static final Component PREFIX = Component.text("History", NamedTextColor.AQUA, TextDecoration.BOLD)
        .append(Component.text("  ", NamedTextColor.DARK_GRAY));

    private HistoryUi() {
    }

    public static Component prefixed(Component content) {
        return PREFIX.append(content);
    }

    public static Component dashboard(boolean inspecting) {
        Component inspectButton = button(
            inspecting ? "조사 끄기" : "블록 조사",
            "/history inspect",
            inspecting ? NamedTextColor.YELLOW : NamedTextColor.GREEN
        );
        return Component.text()
            .append(Component.newline())
            .append(prefixed(Component.text("무엇을 확인할까요?", NamedTextColor.WHITE)))
            .append(Component.newline())
            .append(inspectButton)
            .append(Component.space())
            .append(button("과거 기록 조회", "/history search", NamedTextColor.AQUA))
            .append(Component.space())
            .append(button("안전 되돌리기", "/history rollback * 15m 10", NamedTextColor.GOLD))
            .append(Component.newline())
            .append(button("마지막 작업 취소", "/history undo last", NamedTextColor.LIGHT_PURPLE))
            .append(Component.space())
            .append(button("저장소 상태", "/history status", NamedTextColor.GRAY))
            .append(Component.space())
            .append(button("용량 분석", "/history storage", NamedTextColor.GRAY))
            .append(Component.newline())
            .build();
    }

    public static Component button(String label, String command, NamedTextColor color) {
        return Component.text("[" + label + "]", color, TextDecoration.BOLD)
            .clickEvent(ClickEvent.runCommand(command))
            .hoverEvent(HoverEvent.showText(Component.text("클릭하여 실행", NamedTextColor.GRAY)));
    }

    public static Component historyEntry(ChangeRecord change) {
        String coordinate = change.position().x() + ", " + change.position().y() + ", " + change.position().z();
        String teleportCommand = "/history teleport " + change.position().worldId() + " "
            + change.position().x() + " " + change.position().y() + " " + change.position().z();
        TextComponent.Builder builder = Component.text()
            .append(Component.text(" • " + relativeTime(change.occurredAt()) + "  ", NamedTextColor.DARK_GRAY))
            .append(Component.text(change.actor().name(), NamedTextColor.AQUA))
            .append(Component.text("  " + causeLabel(change.cause()) + "  ", NamedTextColor.GRAY));
        if (change.cause().rollbackEligible()) {
            builder.append(Component.text(material(change.before().materialKey()), NamedTextColor.RED))
                .append(Component.text(" → ", NamedTextColor.DARK_GRAY))
                .append(Component.text(material(change.after().materialKey()), NamedTextColor.GREEN));
        } else {
            builder.append(Component.text(compactMetadata(change.metadata()), NamedTextColor.WHITE));
        }
        return builder
            .hoverEvent(HoverEvent.showText(Component.text(coordinate + " · 클릭하여 이동", NamedTextColor.GRAY)))
            .clickEvent(ClickEvent.runCommand(teleportCommand))
            .build();
    }

    public static Component preview(RollbackPreview preview) {
        long targetChunks = preview.items().stream()
            .map(item -> item.position().worldId() + ":" + item.position().chunkX() + ":" + item.position().chunkZ())
            .distinct()
            .count();
        Component summary = prefixed(Component.text(
            preview.sourceChanges() + "개 기록에서 안전하게 적용 가능한 블록 " + preview.items().size() + "개",
            NamedTextColor.WHITE
        ));
        Component skipped = Component.text(
            "충돌 " + preview.conflicts() + " · 대상 청크 " + targetChunks + " (미로드 자동 로드)"
                + " · 이미 목표 상태 " + preview.alreadyTarget(),
            NamedTextColor.GRAY
        );
        TextComponent.Builder builder = Component.text()
            .append(summary)
            .append(Component.newline())
            .append(Component.text(preview.summary(), NamedTextColor.DARK_GRAY))
            .append(Component.newline())
            .append(skipped);
        if (preview.sourceLimitReached()) {
            builder.append(Component.newline())
                .append(Component.text("검색 상한에 도달했습니다. 범위나 기간을 줄여 주세요.", NamedTextColor.RED));
        }
        if (!preview.items().isEmpty()) {
            builder.append(Component.newline())
                .append(button("확인하고 적용", "/history confirm " + preview.token(), NamedTextColor.RED))
                .append(Component.space())
                .append(button("취소", "/history cancel " + preview.token(), NamedTextColor.GRAY));
        }
        return builder.build();
    }

    public static Component operationResult(OperationRunResult result) {
        NamedTextColor color = result.status().name().equals("APPLIED")
            ? NamedTextColor.GREEN
            : NamedTextColor.YELLOW;
        Component id = Component.text(result.operationId().toString().substring(0, 8), NamedTextColor.AQUA)
            .hoverEvent(HoverEvent.showText(Component.text(result.operationId().toString(), NamedTextColor.GRAY)))
            .clickEvent(ClickEvent.suggestCommand("/history undo " + result.operationId()));
        TextComponent.Builder builder = Component.text()
            .append(prefixed(Component.text("작업 ", NamedTextColor.WHITE)))
            .append(id)
            .append(Component.text(
                " 완료 · 적용 " + result.applied() + " · 건너뜀 " + result.skipped(),
                color
            ));
        if (!result.failure().isBlank()) {
            builder.append(Component.newline())
                .append(Component.text("일부 실패: " + result.failure(), NamedTextColor.RED));
        }
        return builder.build();
    }

    public static Component status(StoreStatus status) {
        NamedTextColor healthColor = status.healthy() ? NamedTextColor.GREEN : NamedTextColor.RED;
        TextComponent.Builder builder = Component.text()
            .append(prefixed(Component.text(
                status.healthy() ? "정상 · " + status.backend() : "확인 필요 · " + status.backend(),
                healthColor
            )))
            .append(Component.newline())
            .append(Component.text(
                "준비 " + yesNo(status.ready())
                    + " · 기록 수락 " + yesNo(status.accepting())
                    + " · 대기 " + status.queued(),
                NamedTextColor.GRAY
            ))
            .append(Component.newline())
            .append(Component.text(
                "수락 " + status.accepted()
                    + " · 저장 " + status.persisted()
                    + " · 복구 등가 병합 " + status.compacted()
                    + " · 거부 " + status.rejected()
                    + " · 만료 정리 " + status.purged(),
                NamedTextColor.GRAY
            ));
        if (status.interruptedOperations() > 0) {
            builder.append(Component.newline())
                .append(Component.text(
                    "검토가 필요한 중단 작업 " + status.interruptedOperations() + "개",
                    NamedTextColor.YELLOW
                ));
        }
        if (!status.lastError().isBlank()) {
            builder.append(Component.newline())
                .append(Component.text(status.lastError(), NamedTextColor.RED));
        }
        return builder.build();
    }

    public static Component storageProfile(StorageProfile profile) {
        TextComponent.Builder builder = Component.text()
            .append(prefixed(Component.text(
                profile.backend() + " · " + formatBytes(profile.totalBytes()),
                NamedTextColor.AQUA
            )))
            .append(Component.newline())
            .append(Component.text(
                "주 저장소 " + formatBytes(profile.databaseBytes())
                    + " · WAL/보조 " + formatBytes(profile.auxiliaryBytes()),
                NamedTextColor.GRAY
            ));
        if (profile.metrics().isEmpty()) {
            return builder.append(Component.newline())
                .append(Component.text(
                    "원인별 계측 데이터가 아직 없습니다. 스키마 업그레이드 후 저장되는 기록부터 누적됩니다.",
                    NamedTextColor.YELLOW
                ))
                .build();
        }
        builder.append(Component.newline())
            .append(Component.text(
                "업그레이드 후 누적 추정 입력량(사전 공유·압축 전 비교값)",
                NamedTextColor.DARK_GRAY
            ));
        profile.metrics().forEach(metric -> builder.append(Component.newline())
            .append(Component.text(
                " • " + causeLabel(metric.cause())
                    + "  " + metric.changeCount() + "건 · "
                    + formatBytes(metric.estimatedInputBytes()) + " · 평균 "
                    + String.format(Locale.ROOT, "%.1f B/건", metric.estimatedBytesPerChange()),
                NamedTextColor.GRAY
            )));
        return builder.build();
    }

    public static String userError(Throwable failure) {
        Throwable current = failure;
        while (current.getCause() != null
            && (current instanceof java.util.concurrent.CompletionException
                || current instanceof java.util.concurrent.ExecutionException)) {
            current = current.getCause();
        }
        String message = current.getMessage();
        return message == null || message.isBlank() ? current.getClass().getSimpleName() : message;
    }

    private static String relativeTime(long occurredAt) {
        long seconds = Math.max(0L, Duration.between(Instant.ofEpochMilli(occurredAt), Instant.now()).toSeconds());
        if (seconds < 60L) {
            return seconds + "초 전";
        }
        if (seconds < 3_600L) {
            return seconds / 60L + "분 전";
        }
        if (seconds < 86_400L) {
            return seconds / 3_600L + "시간 전";
        }
        return seconds / 86_400L + "일 전";
    }

    private static String formatBytes(long bytes) {
        if (bytes < 1_024L) {
            return bytes + " B";
        }
        String[] units = {"KiB", "MiB", "GiB", "TiB"};
        double value = bytes;
        int unit = -1;
        do {
            value /= 1_024.0D;
            unit++;
        } while (value >= 1_024.0D && unit < units.length - 1);
        return String.format(Locale.ROOT, "%.2f %s", value, units[unit]);
    }

    private static String material(String key) {
        int separator = key.indexOf(':');
        String value = separator < 0 ? key : key.substring(separator + 1);
        return value.toLowerCase(Locale.ROOT);
    }

    private static String causeLabel(ChangeCause cause) {
        return switch (cause) {
            case PLAYER_PLACE -> "설치";
            case PLAYER_BREAK -> "파괴";
            case EXPLOSION -> "폭발";
            case FIRE -> "연소";
            case FADE -> "소멸";
            case GROWTH -> "성장";
            case SPREAD -> "확산";
            case FORM -> "생성";
            case LIQUID -> "액체 이동";
            case PISTON -> "피스톤 이동";
            case ENTITY_CHANGE -> "엔티티 변화";
            case HISTORY_ROLLBACK -> "History 되돌리기";
            case HISTORY_UNDO -> "History 작업 취소";
            case WORLD_EDIT -> "WorldEdit/FAWE";
            case PLAYER_INTERACT -> "상호작용";
            case BUCKET -> "양동이";
            case CONTAINER -> "보관함";
            case SIGN -> "표지판";
            case PORTAL -> "포털";
            case PLAYER_SESSION -> "접속";
            case PLAYER_COMMAND -> "명령어";
            case PLAYER_MESSAGE -> "채팅";
            case ITEM_DROP -> "아이템 버림";
            case ITEM_PICKUP -> "아이템 획득";
            case ENTITY_PLACE -> "엔티티 설치";
            case ENTITY_KILL -> "엔티티 처치";
        };
    }

    private static String compactMetadata(String metadata) {
        if (metadata.isBlank()) {
            return "(세부 정보 없음)";
        }
        return metadata.length() <= 120 ? metadata : metadata.substring(0, 117) + "…";
    }

    private static String yesNo(boolean value) {
        return value ? "예" : "아니요";
    }
}
