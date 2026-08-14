package kr.playcity.history.command;

import kr.playcity.history.config.HistoryConfig;
import kr.playcity.history.model.ChangeCause;
import kr.playcity.history.model.ChangeRecord;
import kr.playcity.history.model.HistoryQuery;
import kr.playcity.history.rollback.RollbackPreview;
import kr.playcity.history.rollback.RollbackService;
import kr.playcity.history.storage.HistoryStore;
import kr.playcity.history.storage.StorageProfile;
import kr.playcity.history.util.DurationParser;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.format.NamedTextColor;
import org.bukkit.Bukkit;
import org.bukkit.Location;
import org.bukkit.World;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.bukkit.command.TabCompleter;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

public final class HistoryCommand implements CommandExecutor, TabCompleter {
    private static final List<String> SUBCOMMANDS = List.of(
        "inspect", "lookup", "search", "near", "rollback", "confirm", "cancel", "undo", "status", "storage"
    );
    private final JavaPlugin plugin;
    private final HistoryConfig config;
    private final HistoryStore store;
    private final InspectionService inspection;
    private final RollbackService rollback;
    private final SearchSessionRegistry searches = new SearchSessionRegistry();

    public HistoryCommand(
        JavaPlugin plugin,
        HistoryConfig config,
        HistoryStore store,
        InspectionService inspection,
        RollbackService rollback
    ) {
        this.plugin = plugin;
        this.config = config;
        this.store = store;
        this.inspection = inspection;
        this.rollback = rollback;
    }

    @Override
    public boolean onCommand(CommandSender sender, Command command, String label, String[] args) {
        if (args.length == 0 || args[0].equalsIgnoreCase("help")) {
            if (sender instanceof Player player) {
                player.sendMessage(HistoryUi.dashboard(inspection.isEnabled(player)));
            } else {
                sender.sendMessage(
                    "History: /history lookup t:1d r:10 w:world x:0 z:0 | /history status | /history storage"
                );
            }
            return true;
        }

        String subcommand = args[0].toLowerCase(Locale.ROOT);
        try {
            return switch (subcommand) {
                case "inspect" -> inspect(sender);
                case "lookup", "l" -> lookup(sender, args);
                case "search" -> search(sender, args);
                case "near" -> nearby(sender);
                case "page" -> searchPage(sender, args);
                case "rollback" -> previewRollback(sender, args);
                case "confirm" -> confirm(sender, args);
                case "cancel" -> cancel(sender, args);
                case "undo" -> previewUndo(sender, args);
                case "status" -> status(sender);
                case "storage" -> storage(sender);
                case "teleport" -> teleport(sender, args);
                default -> {
                    sender.sendMessage(HistoryUi.prefixed(Component.text(
                        "알 수 없는 동작입니다. /history 를 입력해 주세요.",
                        NamedTextColor.RED
                    )));
                    yield true;
                }
            };
        } catch (IllegalArgumentException exception) {
            sender.sendMessage(HistoryUi.prefixed(Component.text(exception.getMessage(), NamedTextColor.RED)));
            return true;
        }
    }

    private boolean inspect(CommandSender sender) {
        Player player = requirePlayer(sender);
        requirePermission(player, "history.inspect");
        boolean enabled = inspection.toggle(player);
        player.sendMessage(HistoryUi.prefixed(Component.text(
            enabled ? "조사 모드가 켜졌습니다. 블록을 좌클릭하거나 우클릭하세요."
                : "조사 모드가 꺼졌습니다.",
            enabled ? NamedTextColor.GREEN : NamedTextColor.YELLOW
        )));
        return true;
    }

    private boolean nearby(CommandSender sender) {
        return search(sender, new String[] {"search"});
    }

    private boolean search(CommandSender sender, String[] args) {
        Player player = requirePlayer(sender);
        requirePermission(player, "history.lookup");
        Duration duration = args.length >= 2 ? DurationParser.parse(args[1]) : Duration.ofDays(1);
        int radius = args.length >= 3
            ? parseInteger(args[2], "반경")
            : config.inspection().nearbyRadius();
        if (radius < 1 || radius > config.rollback().maxRadius()) {
            throw new IllegalArgumentException("반경은 1~" + config.rollback().maxRadius() + " 사이여야 합니다.");
        }
        String actor = args.length >= 4 ? args[3] : "*";
        validateActor(actor);
        String causeInput = args.length >= 5 ? args[4] : "all";
        ChangeCause cause = parseCause(causeInput);
        Location center = player.getLocation();
        HistoryQuery query = HistoryQuery.nearby(
            center.getWorld().getUID(),
            center.getBlockX(),
            center.getBlockZ(),
            radius,
            Instant.now().minus(duration).toEpochMilli(),
            actor,
            cause,
            config.inspection().resultLimit() + 1
        );
        String summary = DurationParser.compact(duration) + " · 반경 " + radius
            + (actor.equals("*") ? "" : " · " + actor)
            + (cause == null ? "" : " · " + causeLabel(cause));
        SearchSessionRegistry.SearchSession session = searches.register(
            ownerKey(player), query, summary, config.inspection().resultLimit()
        );
        player.sendMessage(HistoryUi.prefixed(Component.text("과거 기록을 찾는 중…", NamedTextColor.GRAY)));
        runSearch(player, session, query, true, actor, radius, causeInput);
        return true;
    }

    private boolean lookup(CommandSender sender, String[] args) {
        requirePermission(sender, "history.lookup");
        HistoryCliParser.LookupSpec spec = HistoryCliParser.parse(
            java.util.Arrays.copyOfRange(args, 1, args.length),
            config.inspection().nearbyRadius(),
            config.inspection().resultLimit()
        );
        if (spec.radius() > config.rollback().maxRadius()) {
            throw new IllegalArgumentException("반경은 0~" + config.rollback().maxRadius() + " 사이여야 합니다.");
        }
        if (spec.limit() > 50) {
            throw new IllegalArgumentException("한 페이지의 limit은 50 이하여야 합니다.");
        }

        World world = resolveWorld(sender, spec.world());
        int centerX;
        int centerZ;
        if (spec.hasCoordinates()) {
            centerX = spec.x();
            centerZ = spec.z();
        } else if (sender instanceof Player player) {
            centerX = player.getLocation().getBlockX();
            centerZ = player.getLocation().getBlockZ();
        } else {
            throw new IllegalArgumentException("콘솔에서는 w:<월드> x:<X> z:<Z>를 입력해 주세요.");
        }

        long since = Instant.now().minus(spec.duration()).toEpochMilli();
        int queryLimit = spec.limit() + 1;
        HistoryQuery query = spec.exactPosition()
            ? HistoryQuery.at(
                world.getUID(),
                spec.x(),
                spec.y(),
                spec.z(),
                since,
                spec.actor(),
                spec.cause(),
                spec.includedMaterials(),
                spec.excludedMaterials(),
                queryLimit
            )
            : HistoryQuery.nearby(
                world.getUID(),
                centerX,
                centerZ,
                spec.radius(),
                since,
                spec.actor(),
                spec.cause(),
                spec.includedMaterials(),
                spec.excludedMaterials(),
                queryLimit
            );
        String summary = "최근 " + DurationParser.compact(spec.duration())
            + " · " + world.getName()
            + " · " + centerX + ", " + centerZ
            + (spec.exactPosition() ? " · Y " + spec.y() : " · 반경 " + spec.radius())
            + (spec.actor() == null ? "" : " · " + spec.actor())
            + (spec.cause() == null ? "" : " · " + causeLabel(spec.cause()));
        SearchSessionRegistry.SearchSession session = searches.register(
            ownerKey(sender), query, summary, spec.limit()
        );
        sender.sendMessage(HistoryUi.prefixed(Component.text("조건에 맞는 기록을 조회하는 중…", NamedTextColor.GRAY)));
        runSearch(
            sender,
            session,
            query,
            false,
            spec.actor() == null ? "*" : spec.actor(),
            spec.radius(),
            "all"
        );
        return true;
    }

    private boolean searchPage(CommandSender sender, String[] args) {
        requirePermission(sender, "history.lookup");
        if (args.length != 4 || !args[1].matches("[23456789abcdefghjkmnpqrstuvwxyz]{8}")) {
            throw new IllegalArgumentException("조회 버튼이 만료되었거나 올바르지 않습니다.");
        }
        SearchSessionRegistry.SearchSession session = searches.find(args[1], ownerKey(sender))
            .orElseThrow(() -> new IllegalArgumentException("조회 버튼이 만료되었습니다. 새로 조회해 주세요."));
        long occurredAt = parseLong(args[2], "조회 시간");
        long id = parseLong(args[3], "기록 ID");
        if (occurredAt < session.query().since() || id <= 0L) {
            throw new IllegalArgumentException("조회 커서가 올바르지 않습니다.");
        }
        HistoryQuery page = session.query().before(occurredAt, id);
        sender.sendMessage(HistoryUi.prefixed(Component.text("더 오래된 기록을 찾는 중…", NamedTextColor.GRAY)));
        runSearch(sender, session, page, false, "*", page.radius(), "all");
        return true;
    }

    private void runSearch(
        CommandSender sender,
        SearchSessionRegistry.SearchSession session,
        HistoryQuery query,
        boolean firstPage,
        String actor,
        int radius,
        String causeInput
    ) {
        store.query(query).whenComplete((changes, failure) -> onMain(sender, failure, ignored -> {
            int pageSize = session.pageSize();
            boolean hasMore = changes.size() > pageSize;
            List<ChangeRecord> visible = hasMore ? changes.subList(0, pageSize) : changes;
            sender.sendMessage(HistoryUi.prefixed(Component.text(session.summary(), NamedTextColor.WHITE)));
            if (visible.isEmpty()) {
                sender.sendMessage(Component.text(" • 더 이상 기록이 없습니다.", NamedTextColor.GRAY));
                if (firstPage && sender instanceof Player player) {
                    sendSearchQuickFilters(player, actor, radius, causeInput);
                }
                return;
            }
            visible.forEach(change -> sender.sendMessage(HistoryUi.historyEntry(change)));
            if (hasMore) {
                ChangeRecord cursor = visible.get(visible.size() - 1);
                sender.sendMessage(HistoryUi.button(
                    "더 오래된 기록",
                    "/history page " + session.token() + " " + cursor.occurredAt() + " " + cursor.id(),
                    NamedTextColor.AQUA
                ));
            }
            if (firstPage && sender instanceof Player player) {
                sendSearchQuickFilters(player, actor, radius, causeInput);
            }
        }));
    }

    private boolean previewRollback(CommandSender sender, String[] args) {
        Player player = requirePlayer(sender);
        requirePermission(player, "history.rollback");
        String actor = args.length >= 2 ? args[1] : "*";
        validateActor(actor);
        Duration duration = args.length >= 3
            ? DurationParser.parse(args[2])
            : config.rollback().defaultDuration();
        int radius = args.length >= 4 ? parseInteger(args[3], "반경") : config.rollback().defaultRadius();
        if (radius < 1 || radius > config.rollback().maxRadius()) {
            throw new IllegalArgumentException("반경은 1~" + config.rollback().maxRadius() + " 사이여야 합니다.");
        }
        player.sendMessage(HistoryUi.prefixed(Component.text("안전한 되돌리기 범위를 계산하는 중…", NamedTextColor.GRAY)));
        rollback.createRollbackPreview(player, actor, duration, radius)
            .whenComplete((preview, failure) -> onMain(player, failure, ignored -> {
                player.sendMessage(HistoryUi.preview(preview));
                sendQuickFilters(player, actor, radius);
            }));
        return true;
    }

    private boolean confirm(CommandSender sender, String[] args) {
        Player player = requirePlayer(sender);
        if (!player.hasPermission("history.rollback") && !player.hasPermission("history.undo")) {
            throw new IllegalArgumentException("이 작업을 적용할 권한이 없습니다.");
        }
        if (args.length != 2) {
            throw new IllegalArgumentException("확인 버튼이 만료되었습니다. 미리보기를 다시 만들어 주세요.");
        }
        CompletableFuture<kr.playcity.history.rollback.OperationRunResult> future = rollback.apply(player, args[1]);
        player.sendMessage(HistoryUi.prefixed(Component.text(
            "계획을 저장한 뒤 대상 청크만 안전하게 불러와 나누어 적용합니다…",
            NamedTextColor.YELLOW
        )));
        future.whenComplete((result, failure) -> onMain(player, failure, ignored ->
            player.sendMessage(HistoryUi.operationResult(result))
        ));
        return true;
    }

    private boolean cancel(CommandSender sender, String[] args) {
        Player player = requirePlayer(sender);
        if (args.length != 2 || !rollback.cancelPreview(player, args[1])) {
            throw new IllegalArgumentException("미리보기가 없거나 이미 만료되었습니다.");
        }
        player.sendMessage(HistoryUi.prefixed(Component.text("미리보기를 취소했습니다.", NamedTextColor.GRAY)));
        return true;
    }

    private boolean previewUndo(CommandSender sender, String[] args) {
        Player player = requirePlayer(sender);
        requirePermission(player, "history.undo");
        UUID operationId = null;
        if (args.length >= 2 && !args[1].equalsIgnoreCase("last")) {
            try {
                operationId = UUID.fromString(args[1]);
            } catch (IllegalArgumentException exception) {
                throw new IllegalArgumentException("작업 ID가 올바르지 않습니다.", exception);
            }
        }
        player.sendMessage(HistoryUi.prefixed(Component.text("마지막 작업을 검증하는 중…", NamedTextColor.GRAY)));
        rollback.createUndoPreview(player, operationId)
            .whenComplete((preview, failure) -> onMain(player, failure, ignored ->
                player.sendMessage(HistoryUi.preview(preview))
            ));
        return true;
    }

    private boolean status(CommandSender sender) {
        requirePermission(sender, "history.status");
        sender.sendMessage(HistoryUi.status(store.status()));
        return true;
    }

    private boolean storage(CommandSender sender) {
        requirePermission(sender, "history.status");
        sender.sendMessage(HistoryUi.prefixed(Component.text(
            "저장소 용량과 원인별 기록 비용을 계산하는 중…",
            NamedTextColor.GRAY
        )));
        store.storageProfile().whenComplete((profile, failure) -> onMain(sender, failure, ignored ->
            sender.sendMessage(HistoryUi.storageProfile(profile))
        ));
        return true;
    }

    private boolean teleport(CommandSender sender, String[] args) {
        Player player = requirePlayer(sender);
        requirePermission(player, "history.lookup");
        if (args.length != 5) {
            throw new IllegalArgumentException("이동 대상이 올바르지 않습니다.");
        }
        UUID worldId;
        try {
            worldId = UUID.fromString(args[1]);
        } catch (IllegalArgumentException exception) {
            throw new IllegalArgumentException("월드 ID가 올바르지 않습니다.", exception);
        }
        World world = Bukkit.getWorld(worldId);
        if (world == null) {
            throw new IllegalArgumentException("해당 월드가 현재 로드되어 있지 않습니다.");
        }
        int x = parseInteger(args[2], "X");
        int y = parseInteger(args[3], "Y");
        int z = parseInteger(args[4], "Z");
        player.teleportAsync(new Location(world, x + 0.5D, y + 1.0D, z + 0.5D));
        return true;
    }

    private void sendQuickFilters(Player player, String actor, int radius) {
        String safeActor = actor.replace(" ", "");
        Component periods = Component.text("기간  ", NamedTextColor.DARK_GRAY)
            .append(HistoryUi.button("5분", "/history rollback " + safeActor + " 5m " + radius, NamedTextColor.GRAY))
            .append(Component.space())
            .append(HistoryUi.button("15분", "/history rollback " + safeActor + " 15m " + radius, NamedTextColor.GRAY))
            .append(Component.space())
            .append(HistoryUi.button("1시간", "/history rollback " + safeActor + " 1h " + radius, NamedTextColor.GRAY))
            .append(Component.space())
            .append(HistoryUi.button("6시간", "/history rollback " + safeActor + " 6h " + radius, NamedTextColor.GRAY));
        player.sendMessage(periods);
    }

    private void sendSearchQuickFilters(Player player, String actor, int radius, String causeInput) {
        String safeActor = actor.replace(" ", "");
        String safeCause = causeInput.toLowerCase(Locale.ROOT);
        Component periods = Component.text("기간  ", NamedTextColor.DARK_GRAY)
            .append(HistoryUi.button("1시간", "/history search 1h " + radius + " " + safeActor + " " + safeCause, NamedTextColor.GRAY))
            .append(Component.space())
            .append(HistoryUi.button("1일", "/history search 1d " + radius + " " + safeActor + " " + safeCause, NamedTextColor.GRAY))
            .append(Component.space())
            .append(HistoryUi.button("7일", "/history search 7d " + radius + " " + safeActor + " " + safeCause, NamedTextColor.GRAY));
        Component causes = Component.text("원인  ", NamedTextColor.DARK_GRAY)
            .append(HistoryUi.button("전체", "/history search 1d " + radius + " " + safeActor + " all", NamedTextColor.GRAY))
            .append(Component.space())
            .append(HistoryUi.button("WorldEdit/FAWE", "/history search 1d " + radius + " " + safeActor + " worldedit", NamedTextColor.AQUA));
        player.sendMessage(periods);
        player.sendMessage(causes);
    }

    private <T> void onMain(CommandSender sender, Throwable failure, Consumer<T> success) {
        if (!plugin.isEnabled()) {
            return;
        }
        Bukkit.getScheduler().runTask(plugin, () -> {
            if (sender instanceof Player player && !player.isOnline()) {
                return;
            }
            if (failure != null) {
                sender.sendMessage(HistoryUi.prefixed(Component.text(
                    "실패: " + HistoryUi.userError(failure),
                    NamedTextColor.RED
                )));
            } else {
                success.accept(null);
            }
        });
    }

    private static World resolveWorld(CommandSender sender, String requested) {
        if (requested == null) {
            if (sender instanceof Player player) {
                return player.getWorld();
            }
            throw new IllegalArgumentException("콘솔에서는 w:<월드>를 입력해 주세요.");
        }
        World world = null;
        try {
            world = Bukkit.getWorld(UUID.fromString(requested));
        } catch (IllegalArgumentException ignored) {
            // The input can also be a normal world name.
        }
        if (world == null) {
            world = Bukkit.getWorld(requested);
        }
        if (world == null) {
            throw new IllegalArgumentException("로드된 월드를 찾을 수 없습니다: " + requested);
        }
        return world;
    }

    private static String ownerKey(CommandSender sender) {
        if (sender instanceof Player player) {
            return "player:" + player.getUniqueId();
        }
        return sender.getClass().getName() + ":" + sender.getName().toLowerCase(Locale.ROOT);
    }

    private static Player requirePlayer(CommandSender sender) {
        if (!(sender instanceof Player player)) {
            throw new IllegalArgumentException("이 동작은 게임 안에서 사용해 주세요.");
        }
        return player;
    }

    private static void requirePermission(CommandSender sender, String permission) {
        if (!sender.hasPermission(permission)) {
            throw new IllegalArgumentException("이 동작을 사용할 권한이 없습니다.");
        }
    }

    private static int parseInteger(String input, String label) {
        try {
            return Integer.parseInt(input);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(label + " 값이 숫자가 아닙니다.", exception);
        }
    }

    private static long parseLong(String input, String label) {
        try {
            return Long.parseLong(input);
        } catch (NumberFormatException exception) {
            throw new IllegalArgumentException(label + " 값이 숫자가 아닙니다.", exception);
        }
    }

    private static void validateActor(String actor) {
        if (!actor.matches("[A-Za-z0-9_#:\\-]{1,64}")) {
            throw new IllegalArgumentException("대상 이름이 올바르지 않습니다.");
        }
    }

    private static ChangeCause parseCause(String input) {
        return ChangeCauseAliases.parse(input);
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
            case PISTON -> "피스톤";
            case ENTITY_CHANGE -> "엔티티";
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

    @Override
    public List<String> onTabComplete(CommandSender sender, Command command, String alias, String[] args) {
        if (args.length == 1) {
            return matching(SUBCOMMANDS, args[0]);
        }
        if (args[0].equalsIgnoreCase("lookup") || args[0].equalsIgnoreCase("l")) {
            return matching(
                List.of("t:1d", "r:10", "u:", "a:", "i:", "e:", "w:", "x:", "y:", "z:", "limit:10"),
                args[args.length - 1]
            );
        }
        if (args[0].equalsIgnoreCase("rollback")) {
            if (args.length == 2) {
                List<String> actors = new ArrayList<>();
                actors.add("*");
                Bukkit.getOnlinePlayers().forEach(player -> actors.add(player.getName()));
                return matching(actors, args[1]);
            }
            if (args.length == 3) {
                return matching(List.of("5m", "15m", "1h", "6h", "1d"), args[2]);
            }
            if (args.length == 4) {
                return matching(List.of("5", "10", "25", "50", "100", "1000", "10000"), args[3]);
            }
        }
        if (args[0].equalsIgnoreCase("search")) {
            if (args.length == 2) {
                return matching(List.of("15m", "1h", "1d", "7d", "30d"), args[1]);
            }
            if (args.length == 3) {
                return matching(List.of("5", "10", "25", "50", "100", "1000", "10000"), args[2]);
            }
            if (args.length == 4) {
                List<String> actors = new ArrayList<>();
                actors.add("*");
                Bukkit.getOnlinePlayers().forEach(player -> actors.add(player.getName()));
                return matching(actors, args[3]);
            }
            if (args.length == 5) {
                return matching(
                    List.of(
                        "all", "worldedit", "place", "break", "explosion", "fire", "growth", "liquid",
                        "interact", "bucket", "container", "sign", "portal", "session", "command", "drop",
                        "pickup", "entity-place", "entity-kill"
                    ),
                    args[4]
                );
            }
        }
        if (args[0].equalsIgnoreCase("undo") && args.length == 2) {
            return matching(List.of("last"), args[1]);
        }
        return List.of();
    }

    private static List<String> matching(List<String> values, String prefix) {
        String normalized = prefix.toLowerCase(Locale.ROOT);
        return values.stream()
            .filter(value -> value.toLowerCase(Locale.ROOT).startsWith(normalized))
            .toList();
    }
}
