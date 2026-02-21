package com.incoresoft.releasesmigrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.text.Normalizer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class GeneralTablesMigrator implements Migrator {
    private static final List<String> TABLE_ORDER = List.of(
            "analytics_groups", "api_tokens", "cleaning_settings", "clients", "event_manager",
            "face_lists", "face_detections", "face_notifications", "plugin_configurations", "roles",
            "servers", "sounds_settings", "stats_traffic_minutely", "stream_groups", "streams",
            "system_settings", "traffic_counters", "traffic_stat", "users", "analytics", "heatmap_plans"
    );

    private static final Set<String> DROPPED_TABLES = Set.of(
            "gender_age_stat", "gun_notifications", "gun_type_mapping", "hardhats_notifications",
            "object_in_zone_notifications", "smoke_fire_notifications", "smoke_fire_type_mapping",
            "zone_exit_notifications", "zone_exit_notifications_object_type", "object_in_zone_object_type",
            "face_list_items", "face_list_items_images"
    );

    @Override
    public void migrate() {
        MigrationConfig config = loadConfig();
        DumpData dumpData = DumpParser.parseDump(Path.of(config.getSource().getDumpPath()));
        JdbcTemplate targetJdbc = new JdbcTemplate(buildTargetDataSource(config));
        TransactionTemplate tx = new TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(buildTargetDataSource(config)));

        Map<Object, String> streamUuidByRef = buildStreamUuidMap(dumpData.rowsByTable().getOrDefault("streams", List.of()));

        for (String table : TABLE_ORDER) {
            if (DROPPED_TABLES.contains(table)) {
                continue;
            }
            String targetTable = table.equals("heatmap_plans") ? "smart_va_heatmap_plans" : table;
            List<DumpParser.Row> rows = dumpData.rowsByTable().getOrDefault(table, List.of());
            if (rows.isEmpty()) {
                continue;
            }
            tx.execute(status -> {
                List<DumpParser.Row> prepared = prepareRows(table, rows, streamUuidByRef);
                if (!prepared.isEmpty()) {
                    insertRows(targetJdbc, config.getTarget().getType(), targetTable, prepared);
                    syncSequence(targetJdbc, config.getTarget().getType(), targetTable);
                }
                return null;
            });
            log.info("Migrated table {} -> {} rows: {}", table, targetTable, rows.size());
        }

        migrateFaceListImages(config, dumpData);
    }

    private List<DumpParser.Row> prepareRows(String table, List<DumpParser.Row> rows, Map<Object, String> streamUuidByRef) {
        List<DumpParser.Row> out = new ArrayList<>();
        for (DumpParser.Row row : rows) {
            Map<String, Object> values = new LinkedHashMap<>(row.values());
            if (isExcludedByStatus(values)) {
                continue;
            }
            if (table.equals("streams") && blank(values.get("uuid"))) {
                values.put("uuid", deterministicUuid("stream", values.get("id")));
            }
            if (table.equals("analytics")) {
                if (blank(values.get("uuid"))) {
                    values.put("uuid", deterministicUuid("analytics", values.get("id")));
                }
                Object streamRef = values.get("stream");
                String streamUuid = streamUuidByRef.get(streamRef);
                if (streamUuid == null && streamRef != null) {
                    streamUuid = streamUuidByRef.get(String.valueOf(streamRef));
                }
                if (streamUuid != null) {
                    values.put("stream_uuid", streamUuid);
                }
                values.putIfAbsent("group_id", 0L);
                if (values.get("group_id") == null) {
                    values.put("group_id", 0L);
                }
            }
            values.replaceAll((k, v) -> normalizeValue(k, v));
            out.add(new DumpParser.Row(row.table(), values));
        }
        return out;
    }

    private void insertRows(JdbcTemplate jdbcTemplate, String targetType, String table, List<DumpParser.Row> rows) {
        List<String> columns = new ArrayList<>(rows.getFirst().values().keySet());
        int chunkSize = 200;
        for (int i = 0; i < rows.size(); i += chunkSize) {
            List<DumpParser.Row> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
            String placeholders = "(" + String.join(",", Collections.nCopies(columns.size(), "?")) + ")";
            String sql = "INSERT INTO " + table + " (" + String.join(",", columns) + ") VALUES " +
                    chunk.stream().map(r -> placeholders).collect(Collectors.joining(",")) + duplicateClause(targetType, table);

            List<Object> params = new ArrayList<>();
            for (DumpParser.Row row : chunk) {
                for (String col : columns) {
                    params.add(row.values().get(col));
                }
            }
            jdbcTemplate.update(sql, params.toArray());
        }
    }

    private void syncSequence(JdbcTemplate jdbcTemplate, String targetType, String table) {
        if (!"postgres".equalsIgnoreCase(targetType)) {
            return;
        }
        try {
            jdbcTemplate.execute("SELECT setval('" + table + "_id_seq', COALESCE((SELECT MAX(id) FROM " + table + "), 1), true)");
        } catch (Exception e) {
            log.debug("Sequence sync skipped for {}: {}", table, e.getMessage());
        }
    }

    private String duplicateClause(String targetType, String table) {
        if ("postgres".equalsIgnoreCase(targetType)) {
            return " ON CONFLICT DO NOTHING";
        }
        return " ON DUPLICATE KEY UPDATE id=id";
    }

    private DataSource buildTargetDataSource(MigrationConfig config) {
        DriverManagerDataSource ds = new DriverManagerDataSource();
        String type = config.getTarget().getType();
        if ("postgres".equalsIgnoreCase(type)) {
            ds.setDriverClassName("org.postgresql.Driver");
        } else if ("mysql".equalsIgnoreCase(type)) {
            ds.setDriverClassName("com.mysql.cj.jdbc.Driver");
        } else {
            throw new IllegalArgumentException("Unsupported target type: " + type);
        }
        ds.setUrl(config.getTarget().getJdbcUrl());
        ds.setUsername(config.getTarget().getUser());
        ds.setPassword(config.getTarget().getPassword());
        return ds;
    }

    private MigrationConfig loadConfig() {
        String explicit = System.getProperty("config.path", System.getenv().getOrDefault("MIGRATOR_CONFIG_PATH", "config.json"));
        try {
            return new ObjectMapper().readValue(Path.of(explicit).toFile(), MigrationConfig.class);
        } catch (IOException e) {
            throw new RuntimeException("Cannot read config from " + explicit, e);
        }
    }

    private Map<Object, String> buildStreamUuidMap(List<DumpParser.Row> streamRows) {
        Map<Object, String> map = new HashMap<>();
        for (DumpParser.Row row : streamRows) {
            Map<String, Object> values = row.values();
            String uuid = values.get("uuid") == null || String.valueOf(values.get("uuid")).isBlank()
                    ? deterministicUuid("stream", values.get("id"))
                    : String.valueOf(values.get("uuid"));
            Object id = values.get("id");
            if (id != null) map.put(id, uuid);
            map.put(String.valueOf(id), uuid);
            addMap(values.get("path"), uuid, map);
            addMap(values.get("name"), uuid, map);
            addMap(values.get("file_name"), uuid, map);
        }
        return map;
    }

    private void addMap(Object key, String value, Map<Object, String> map) {
        if (key == null) return;
        String s = String.valueOf(key);
        map.put(s, value);
    }

    private boolean isExcludedByStatus(Map<String, Object> values) {
        Object status = values.get("status");
        if (status == null) return false;
        return "-1".equals(String.valueOf(status));
    }

    private Object normalizeValue(String column, Object value) {
        if (!(value instanceof String s)) {
            return value;
        }
        String trimmed = s.trim();
        if (trimmed.isEmpty() || "-".equals(trimmed) || "NULL".equalsIgnoreCase(trimmed)) {
            return null;
        }
        if (isSensitive(column)) {
            return trimmed;
        }
        return normalizeAscii(trimmed);
    }

    private boolean isSensitive(String column) {
        String c = column.toLowerCase(Locale.ROOT);
        return c.contains("password") || c.contains("hash") || c.contains("token") || c.contains("secret") || c.contains("signature") || c.contains("base64") || c.equals("api_key");
    }

    private String normalizeAscii(String in) {
        String withSubs = in.replace("ß", "ss").replace("æ", "ae");
        String norm = Normalizer.normalize(withSubs, Normalizer.Form.NFKD).replaceAll("\\p{M}+", "");
        return norm;
    }

    private boolean blank(Object value) {
        return value == null || String.valueOf(value).isBlank();
    }

    private String deterministicUuid(String scope, Object id) {
        return UUID.nameUUIDFromBytes((scope + ":" + String.valueOf(id)).getBytes(StandardCharsets.UTF_8)).toString();
    }

    private void migrateFaceListImages(MigrationConfig config, DumpData dumpData) {
        List<DumpParser.Row> lists = dumpData.rowsByTable().getOrDefault("face_lists", List.of());
        List<DumpParser.Row> items = dumpData.rowsByTable().getOrDefault("face_list_items", List.of());
        List<DumpParser.Row> images = dumpData.rowsByTable().getOrDefault("face_list_items_images", List.of());
        if (lists.isEmpty() || items.isEmpty() || images.isEmpty()) {
            log.info("Skipping face image move: missing required face tables in dump.");
            return;
        }

        Map<Object, String> listNames = lists.stream().collect(Collectors.toMap(r -> r.values().get("id"), r -> safeFolderName(String.valueOf(r.values().get("name"))), (a, b) -> a));
        Map<Object, DumpParser.Row> itemById = items.stream().collect(Collectors.toMap(r -> r.values().get("id"), r -> r, (a, b) -> a));

        Path sourceDir = Path.of(Optional.ofNullable(config.getImages()).map(MigrationConfig.ImagesConfig::getSourceDir).orElse("./face_lists"));
        Path targetDir = Path.of(Optional.ofNullable(config.getImages()).map(MigrationConfig.ImagesConfig::getTargetDir).orElse("./face_lists_new"));

        int moved = 0;
        for (DumpParser.Row imageRow : images) {
            Object listItemId = imageRow.values().get("list_item_id");
            DumpParser.Row item = itemById.get(listItemId);
            if (item == null || isExcludedByStatus(item.values())) {
                continue;
            }
            Object listId = item.values().get("list_id");
            String listFolder = listNames.getOrDefault(listId, "list_" + listId);
            String personName = safeFileName(String.valueOf(item.values().getOrDefault("name", "unknown")));
            String rawPath = String.valueOf(imageRow.values().getOrDefault("path", ""));
            String fileName = Path.of(rawPath).getFileName().toString();
            String extension = fileName.contains(".") ? fileName.substring(fileName.lastIndexOf('.')) : "";

            Path src = sourceDir.resolve(fileName);
            Path dstDir = targetDir.resolve(listFolder);
            Path dst = uniquePath(dstDir, personName + extension);

            try {
                Files.createDirectories(dstDir);
                if (Files.exists(src)) {
                    Files.move(src, dst, StandardCopyOption.REPLACE_EXISTING);
                    moved++;
                } else {
                    log.warn("Face image source not found: {}", src);
                }
            } catch (Exception e) {
                log.warn("Failed to move image {} -> {}: {}", src, dst, e.getMessage());
            }
        }
        log.info("Face list image move completed. moved={}", moved);
    }

    private Path uniquePath(Path dir, String baseName) {
        Path candidate = dir.resolve(baseName);
        if (!Files.exists(candidate)) {
            return candidate;
        }
        String name = baseName;
        String ext = "";
        if (baseName.contains(".")) {
            ext = baseName.substring(baseName.lastIndexOf('.'));
            name = baseName.substring(0, baseName.lastIndexOf('.'));
        }
        int i = 1;
        while (Files.exists(candidate)) {
            candidate = dir.resolve(name + "_" + i + ext);
            i++;
        }
        return candidate;
    }

    private String safeFolderName(String input) {
        return safeFileName(input == null ? "unknown_list" : input).replace(' ', '_');
    }

    private String safeFileName(String input) {
        String normalized = normalizeAscii(input == null ? "unknown" : input).trim();
        if (normalized.isEmpty()) {
            normalized = "unknown";
        }
        return normalized.replaceAll("[\\\\/:*?\"<>|]", "_");
    }

    private record DumpData(Map<String, List<DumpParser.Row>> rowsByTable) {
    }

    private static final class DumpParser {
        static DumpData parseDump(Path path) {
            try {
                String sql = Files.readString(path);
                Map<String, List<Row>> byTable = new LinkedHashMap<>();
                for (String statement : splitStatements(sql)) {
                    String t = statement.trim();
                    if (!t.regionMatches(true, 0, "INSERT INTO", 0, "INSERT INTO".length())) {
                        continue;
                    }
                    ParsedInsert parsed = parseInsert(t);
                    if (parsed == null) continue;
                    byTable.computeIfAbsent(parsed.table(), k -> new ArrayList<>()).addAll(parsed.rows());
                }
                return new DumpData(byTable);
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse dump: " + path, e);
            }
        }

        private static List<String> splitStatements(String sql) {
            List<String> out = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuote = false;
            for (int i = 0; i < sql.length(); i++) {
                char c = sql.charAt(i);
                current.append(c);
                if (c == '\'' && (i == 0 || sql.charAt(i - 1) != '\\')) {
                    inQuote = !inQuote;
                }
                if (c == ';' && !inQuote) {
                    out.add(current.toString());
                    current.setLength(0);
                }
            }
            if (!current.isEmpty()) {
                out.add(current.toString());
            }
            return out;
        }

        private static ParsedInsert parseInsert(String statement) {
            int intoIdx = statement.toUpperCase(Locale.ROOT).indexOf("INSERT INTO") + "INSERT INTO".length();
            int firstParen = statement.indexOf('(', intoIdx);
            if (firstParen < 0) return null;
            String tableRaw = statement.substring(intoIdx, firstParen).trim().replace("`", "");
            if (tableRaw.contains(".")) {
                tableRaw = tableRaw.substring(tableRaw.lastIndexOf('.') + 1);
            }

            int colEnd = findMatchingParen(statement, firstParen);
            List<String> columns = Arrays.stream(statement.substring(firstParen + 1, colEnd).split(","))
                    .map(s -> s.replace("`", "").trim())
                    .toList();

            int valuesIdx = statement.toUpperCase(Locale.ROOT).indexOf("VALUES", colEnd);
            if (valuesIdx < 0) return null;
            String valuesPart = statement.substring(valuesIdx + 6).trim();
            if (valuesPart.endsWith(";")) valuesPart = valuesPart.substring(0, valuesPart.length() - 1);

            List<Row> rows = new ArrayList<>();
            for (String tuple : splitTuples(valuesPart)) {
                List<String> valueTokens = splitTopLevel(tuple.substring(1, tuple.length() - 1));
                Map<String, Object> values = new LinkedHashMap<>();
                for (int i = 0; i < columns.size() && i < valueTokens.size(); i++) {
                    values.put(columns.get(i), parseValue(valueTokens.get(i).trim()));
                }
                rows.add(new Row(tableRaw.toLowerCase(Locale.ROOT), values));
            }
            return new ParsedInsert(tableRaw.toLowerCase(Locale.ROOT), rows);
        }

        private static int findMatchingParen(String s, int openPos) {
            int depth = 0;
            boolean inQuote = false;
            for (int i = openPos; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '\'' && (i == 0 || s.charAt(i - 1) != '\\')) inQuote = !inQuote;
                if (inQuote) continue;
                if (c == '(') depth++;
                if (c == ')') {
                    depth--;
                    if (depth == 0) return i;
                }
            }
            return -1;
        }

        private static List<String> splitTuples(String valuesPart) {
            List<String> tuples = new ArrayList<>();
            int depth = 0;
            boolean inQuote = false;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < valuesPart.length(); i++) {
                char c = valuesPart.charAt(i);
                sb.append(c);
                if (c == '\'' && (i == 0 || valuesPart.charAt(i - 1) != '\\')) inQuote = !inQuote;
                if (inQuote) continue;
                if (c == '(') depth++;
                if (c == ')') depth--;
                if (c == ')' && depth == 0) {
                    tuples.add(sb.toString().trim());
                    sb.setLength(0);
                    while (i + 1 < valuesPart.length() && (valuesPart.charAt(i + 1) == ',' || Character.isWhitespace(valuesPart.charAt(i + 1)))) i++;
                }
            }
            return tuples;
        }

        private static List<String> splitTopLevel(String s) {
            List<String> parts = new ArrayList<>();
            StringBuilder cur = new StringBuilder();
            boolean inQuote = false;
            int nested = 0;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c == '\'' && (i == 0 || s.charAt(i - 1) != '\\')) inQuote = !inQuote;
                if (!inQuote) {
                    if (c == '(') nested++;
                    if (c == ')') nested--;
                    if (c == ',' && nested == 0) {
                        parts.add(cur.toString());
                        cur.setLength(0);
                        continue;
                    }
                }
                cur.append(c);
            }
            parts.add(cur.toString());
            return parts;
        }

        private static Object parseValue(String token) {
            if (token.equalsIgnoreCase("NULL")) return null;
            if (token.startsWith("'") && token.endsWith("'")) {
                String inner = token.substring(1, token.length() - 1);
                return inner.replace("\\'", "'").replace("''", "'").replace("\\\\", "\\");
            }
            if (token.startsWith("0x")) {
                return token;
            }
            if (token.matches("-?\\d+")) {
                try {
                    return Long.parseLong(token);
                } catch (NumberFormatException e) {
                    return token;
                }
            }
            if (token.matches("-?\\d+\\.\\d+")) {
                try {
                    return Double.parseDouble(token);
                } catch (NumberFormatException e) {
                    return token;
                }
            }
            return token;
        }

        record ParsedInsert(String table, List<Row> rows) {
        }

        record Row(String table, Map<String, Object> values) {
        }
    }
}
