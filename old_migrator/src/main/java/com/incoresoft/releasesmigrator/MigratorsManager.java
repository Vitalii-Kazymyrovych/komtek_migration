package com.incoresoft.releasesmigrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.Reader;
import java.io.IOException;
import java.nio.charset.MalformedInputException;
import java.nio.charset.StandardCharsets;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.sql.*;
import java.text.Normalizer;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

@Slf4j
@Component
@RequiredArgsConstructor
public class MigratorsManager {
    private static final List<Charset> DUMP_CHARSETS = List.of(
            StandardCharsets.UTF_8,
            Charset.forName("windows-1251"),
            StandardCharsets.ISO_8859_1
    );
    private static final List<String> TABLE_ORDER = List.of(
            "clients",
            "roles",
            "users",
            "stream_groups",
            "streams",
            "analytics_groups",
            "analytics",
            "traffic_counters",
            "traffic_stat",
            "event_manager",
            "alpr_lists",
            "alpr_list_items",
            "face_lists",
            "face_list_items",
            "face_list_items_images"
    );
    private static final Pattern SENSITIVE = Pattern.compile("(?i)(password|hash|token|secret|signature|base64)");
    private static final Pattern DUMP_STATEMENT_SKIP = Pattern.compile("(?is)^\\s*(LOCK\\s+TABLES|UNLOCK\\s+TABLES|DELIMITER)\\b.*");
    private static final Pattern DUMP_ENGINE_SUFFIX = Pattern.compile("(?is)\\s+ENGINE\\s*=\\s*[^\\s;]+(?:\\s+AUTO_INCREMENT\\s*=\\s*\\d+)?");
    private static final Pattern DUMP_DEFAULT_CHARSET_SUFFIX = Pattern.compile("(?is)\\s+DEFAULT\\s+CHARSET\\s*=\\s*[^\\s;]+");

    private final ObjectMapper objectMapper;

    public void migrate() {
        MigrationConfig config = readConfig();
        try (Connection sourceConnection = openSourceConnection(config);
             Connection targetConnection = openTargetConnection(config)) {
            targetConnection.setAutoCommit(false);

            for (String table : TABLE_ORDER) {
                migrateTable(sourceConnection, targetConnection, table);
            }

            targetConnection.commit();
            renameFaceImages(config, targetConnection);
            log.info("Migration completed at {}", Instant.now());
        } catch (Exception e) {
            throw new IllegalStateException("Migration failed", e);
        }
    }

    private MigrationConfig readConfig() {
        Path configPath = Path.of("config.json");
        if (!Files.exists(configPath)) {
            throw new IllegalStateException("config.json was not found near migrator.jar");
        }
        try {
            JsonNode root = objectMapper.readTree(Files.readString(configPath));
            JsonNode source = root.path("source");
            JsonNode target = root.path("target");
            JsonNode images = root.path("images");
            return new MigrationConfig(
                    source.path("type").asText("mysql_dump"),
                    source.path("dump_path").asText(),
                    target.path("type").asText("postgres"),
                    target.path("jdbc_url").asText(),
                    target.path("user").asText(),
                    target.path("password").asText(),
                    images.path("source_dir").asText(""),
                    images.path("target_dir").asText(""));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to parse config.json", e);
        }
    }

    private Connection openSourceConnection(MigrationConfig config) throws Exception {
        Class.forName("org.h2.Driver");
        String jdbc = "jdbc:h2:mem:legacy;MODE=" + (config.sourceType.equalsIgnoreCase("mysql_dump") ? "MySQL" : "Regular") + ";DATABASE_TO_LOWER=TRUE;NON_KEYWORDS=VALUE";
        Connection connection = DriverManager.getConnection(jdbc, "sa", "");
        executeDumpSqlStreaming(Path.of(config.dumpPath), connection);
        return connection;
    }

    private void executeDumpSqlStreaming(Path dumpPath, Connection connection) throws IOException {
        IOException lastException = null;
        for (Charset charset : DUMP_CHARSETS) {
            try {
                executeDumpSqlStreaming(dumpPath, connection, charset);
                if (!StandardCharsets.UTF_8.equals(charset)) {
                    log.warn("Decoded dump {} with fallback charset {}", dumpPath, charset);
                }
                return;
            } catch (MalformedInputException ex) {
                lastException = ex;
                log.warn("Failed to decode dump {} as {}: {}", dumpPath, charset, ex.getMessage());
            }
        }
        throw new IOException("Failed to decode dump with supported charsets: " + dumpPath, lastException);
    }

    private void executeDumpSqlStreaming(Path dumpPath, Connection connection, Charset charset) throws IOException {
        StringBuilder statementBuilder = new StringBuilder(8 * 1024);
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        boolean inLineComment = false;
        boolean inBlockComment = false;

        try (Reader reader = Files.newBufferedReader(dumpPath, charset)) {
            char[] buffer = new char[16 * 1024];
            int read;
            while ((read = reader.read(buffer)) != -1) {
                for (int i = 0; i < read; i++) {
                    char current = buffer[i];
                    char next = i + 1 < read ? buffer[i + 1] : '\0';
                    char previous = statementBuilder.isEmpty() ? '\0' : statementBuilder.charAt(statementBuilder.length() - 1);

                    if (inLineComment) {
                        if (current == '\n' || current == '\r') {
                            inLineComment = false;
                        }
                        continue;
                    }

                    if (inBlockComment) {
                        if (current == '*' && next == '/') {
                            inBlockComment = false;
                            i++;
                        }
                        continue;
                    }

                    if (!inSingleQuote && !inDoubleQuote) {
                        if (current == '#' ) {
                            inLineComment = true;
                            continue;
                        }
                        if (current == '-' && next == '-' && Character.isWhitespace(previous == '\0' ? ' ' : previous)) {
                            inLineComment = true;
                            i++;
                            continue;
                        }
                        if (current == '/' && next == '*') {
                            inBlockComment = true;
                            i++;
                            continue;
                        }
                    }

                    if (current == '\'' && !inDoubleQuote && previous != '\\') {
                        inSingleQuote = !inSingleQuote;
                    } else if (current == '"' && !inSingleQuote && previous != '\\') {
                        inDoubleQuote = !inDoubleQuote;
                    }

                    if (current == '`' && !inSingleQuote && !inDoubleQuote) {
                        continue;
                    }

                    if (current == ';' && !inSingleQuote && !inDoubleQuote) {
                        executeSourceStatement(connection, statementBuilder.toString());
                        statementBuilder.setLength(0);
                        continue;
                    }

                    statementBuilder.append(current);
                }
            }
        }

        executeSourceStatement(connection, statementBuilder.toString());
    }

    private void executeSourceStatement(Connection connection, String statement) {
        String sql = sanitizeDumpStatement(statement);
        if (sql.isBlank()) {
            return;
        }
        try (Statement st = connection.createStatement()) {
            st.execute(sql);
        } catch (SQLException ex) {
            log.debug("Skipped source statement: {}", ex.getMessage());
        }
    }

    private String sanitizeDumpStatement(String statement) {
        String sql = statement.trim();
        if (sql.isBlank()) {
            return "";
        }
        if (DUMP_STATEMENT_SKIP.matcher(sql).matches()) {
            return "";
        }

        sql = DUMP_ENGINE_SUFFIX.matcher(sql).replaceAll("");
        sql = DUMP_DEFAULT_CHARSET_SUFFIX.matcher(sql).replaceAll("");
        return sql.trim();
    }

    private Connection openTargetConnection(MigrationConfig config) throws Exception {
        String targetType = config.targetType.toLowerCase(Locale.ROOT);
        if ("mysql".equals(targetType)) {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } else {
            Class.forName("org.postgresql.Driver");
        }
        return DriverManager.getConnection(config.targetJdbcUrl, config.targetUser, config.targetPassword);
    }

    private void migrateTable(Connection source, Connection target, String table) throws SQLException {
        if ("analytics".equals(table)) {
            migrateAnalyticsWithLegacyIds(source, target);
            return;
        }

        if (!tableExists(source, table) || !tableExists(target, table)) {
            log.info("Skipping table {} because it does not exist in source or target", table);
            return;
        }

        List<String> targetColumns = tableColumns(target, table);
        List<String> sourceColumns = tableColumns(source, table);
        List<String> columns = targetColumns.stream().filter(sourceColumns::contains).toList();
        if (columns.isEmpty()) {
            log.info("Skipping table {} because there are no common columns", table);
            return;
        }

        String where = sourceColumns.contains("status") ? " WHERE status <> -1" : "";
        String selectSql = "SELECT " + String.join(",", columns) + " FROM " + table + where;

        String insertSql = "INSERT INTO " + table + " (" + String.join(",", columns) + ") VALUES (" + String.join(",", Collections.nCopies(columns.size(), "?")) + ") " + duplicateIgnoreClause(configuredTargetType(target));

        int migrated = 0;
        try (PreparedStatement select = source.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = target.prepareStatement(insertSql)) {
            while (rs.next()) {
                for (int i = 0; i < columns.size(); i++) {
                    String column = columns.get(i);
                    Object value = rs.getObject(i + 1);
                    value = normalizeValue(table, column, value);
                    insert.setObject(i + 1, value);
                }
                insert.addBatch();
                migrated++;
                if (migrated % 500 == 0) {
                    insert.executeBatch();
                }
            }
            insert.executeBatch();
        }

        syncIdentitySequence(target, table);
        log.info("Migrated {} rows into {}", migrated, table);
    }

    private void migrateAnalyticsWithLegacyIds(Connection source, Connection target) throws SQLException {
        final String table = "analytics";
        if (!tableExists(source, table) || !tableExists(target, table)) {
            log.info("Skipping table {} because it does not exist in source or target", table);
            return;
        }

        List<String> targetColumns = tableColumns(target, table);
        List<String> sourceColumns = tableColumns(source, table);
        Set<String> sourceColumnSet = new HashSet<>(sourceColumns);

        List<String> commonColumns = targetColumns.stream()
                .filter(sourceColumnSet::contains)
                .filter(column -> !"uuid".equals(column))
                .toList();

        List<String> insertColumns = new ArrayList<>(commonColumns);
        if (!insertColumns.contains("uuid")) {
            insertColumns.add("uuid");
        }
        if (targetColumns.contains("stream_uuid") && !insertColumns.contains("stream_uuid")) {
            insertColumns.add("stream_uuid");
        }

        String selectSql = "SELECT " + String.join(",", commonColumns) + " FROM " + table
                + (sourceColumnSet.contains("status") ? " WHERE status <> -1" : "");
        String insertSql = "INSERT INTO " + table + " (" + String.join(",", insertColumns)
                + ") VALUES (" + String.join(",", Collections.nCopies(insertColumns.size(), "?"))
                + ") " + duplicateIgnoreClause(configuredTargetType(target));

        Map<Integer, UUID> streamIdToUuid = loadStreamUuidMap(source);

        int migrated = 0;
        try (PreparedStatement select = source.prepareStatement(selectSql);
             ResultSet rs = select.executeQuery();
             PreparedStatement insert = target.prepareStatement(insertSql)) {
            while (rs.next()) {
                Map<String, Object> rowValues = new HashMap<>();
                for (int i = 0; i < commonColumns.size(); i++) {
                    String column = commonColumns.get(i);
                    Object value = normalizeValue(table, column, rs.getObject(i + 1));
                    rowValues.put(column, value);
                }

                Integer analyticsId = asInteger(rowValues.get("id"));
                UUID analyticsUuid = parseUuid(rowValues.get("uuid"));
                if (analyticsUuid == null) {
                    analyticsUuid = deterministicUuid("analytics", analyticsId);
                }
                rowValues.put("uuid", analyticsUuid);

                if (insertColumns.contains("stream_uuid")) {
                    UUID streamUuid = parseUuid(rowValues.get("stream_uuid"));
                    if (streamUuid == null && sourceColumnSet.contains("stream_id")) {
                        Integer streamId = asInteger(rowValues.get("stream_id"));
                        streamUuid = streamId == null ? null : streamIdToUuid.get(streamId);
                    }
                    rowValues.put("stream_uuid", streamUuid);
                }

                for (int i = 0; i < insertColumns.size(); i++) {
                    insert.setObject(i + 1, rowValues.get(insertColumns.get(i)));
                }
                insert.addBatch();
                migrated++;
                if (migrated % 500 == 0) {
                    insert.executeBatch();
                }
            }
            insert.executeBatch();
        }

        syncIdentitySequence(target, table);
        log.info("Migrated {} rows into {} with legacy ids", migrated, table);
    }

    private Map<Integer, UUID> loadStreamUuidMap(Connection source) {
        Map<Integer, UUID> map = new HashMap<>();
        try {
            if (!tableExists(source, "streams")) {
                return map;
            }
            List<String> streamColumns = tableColumns(source, "streams");
            if (!streamColumns.contains("id") || !streamColumns.contains("uuid")) {
                return map;
            }
            try (PreparedStatement ps = source.prepareStatement("SELECT id, uuid FROM streams");
                 ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    UUID streamUuid = parseUuid(rs.getObject("uuid"));
                    if (streamUuid != null) {
                        map.put(rs.getInt("id"), streamUuid);
                    }
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to preload stream UUID mapping for analytics migration: {}", e.getMessage());
        }
        return map;
    }

    private void syncIdentitySequence(Connection target, String table) {
        try {
            String targetType = configuredTargetType(target);
            List<String> columns = tableColumns(target, table);
            if (!columns.contains("id")) {
                return;
            }
            if ("mysql".equals(targetType)) {
                long nextId = 1;
                try (PreparedStatement ps = target.prepareStatement("SELECT GREATEST(COALESCE(MAX(id), 0) + 1, 1) FROM " + table);
                     ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        nextId = rs.getLong(1);
                    }
                }
                try (Statement st = target.createStatement()) {
                    st.execute("ALTER TABLE " + table + " AUTO_INCREMENT = " + nextId);
                }
            } else {
                String sql = "SELECT setval(pg_get_serial_sequence(?, 'id'), GREATEST((SELECT COALESCE(MAX(id), 0) FROM " + table + "), 1), true)";
                try (PreparedStatement ps = target.prepareStatement(sql)) {
                    ps.setString(1, table);
                    ps.execute();
                }
            }
        } catch (Exception e) {
            log.debug("Sequence sync skipped for {}: {}", table, e.getMessage());
        }
    }

    private String duplicateIgnoreClause(String targetType) {
        return "mysql".equals(targetType) ? "ON DUPLICATE KEY UPDATE id = id" : "ON CONFLICT DO NOTHING";
    }

    private String configuredTargetType(Connection target) {
        try {
            String dbName = target.getMetaData().getDatabaseProductName();
            if (dbName != null && dbName.toLowerCase(Locale.ROOT).contains("mysql")) {
                return "mysql";
            }
        } catch (SQLException ignored) {
            // fallback below
        }
        return "postgres";
    }

    private Integer asInteger(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text && !text.isBlank()) {
            try {
                return Integer.parseInt(text.trim());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private UUID parseUuid(Object value) {
        if (value == null) {
            return null;
        }
        try {
            return UUID.fromString(String.valueOf(value).trim());
        } catch (Exception e) {
            return null;
        }
    }

    private UUID deterministicUuid(String namespace, Integer id) {
        String raw = namespace + ":" + (id == null ? "null" : id);
        return UUID.nameUUIDFromBytes(raw.getBytes(StandardCharsets.UTF_8));
    }

    private void renameFaceImages(MigrationConfig config, Connection target) {
        if (config.imageSourceDir.isBlank() || config.imageTargetDir.isBlank()) {
            log.info("Image rename is skipped (source_dir/target_dir are not set)");
            return;
        }
        Path sourceDir = Path.of(config.imageSourceDir);
        Path targetDir = Path.of(config.imageTargetDir);

        if (!Files.isDirectory(sourceDir)) {
            log.warn("Image source dir does not exist: {}", sourceDir);
            return;
        }

        try {
            String faceItemsQuery = resolveFaceImageSelectQuery(target);
            if (faceItemsQuery == null) {
                log.info("Image rename is skipped (neither face_list_items.image nor face_list_items_images.path is available)");
                return;
            }

            try (PreparedStatement ps = target.prepareStatement(faceItemsQuery);
                 ResultSet rs = ps.executeQuery()) {
                Map<String, FaceItem> imageToItem = new HashMap<>();
                while (rs.next()) {
                    String image = rs.getString("image");
                    if (image != null && !image.isBlank()) {
                        String normalizedImageName = extractFileName(image);
                        int imageOrder = rs.getInt("image_order");
                        FaceItem candidate = new FaceItem(rs.getLong("id"), rs.getLong("list_id"), normalizedImageName, imageOrder);
                        imageToItem.compute(normalizedImageName, (key, existing) -> {
                            if (existing == null) {
                                return candidate;
                            }
                            if (candidate.listId < existing.listId) {
                                return candidate;
                            }
                            if (candidate.listId == existing.listId && candidate.id < existing.id) {
                                return candidate;
                            }
                            if (candidate.listId == existing.listId && candidate.id == existing.id && candidate.imageOrder < existing.imageOrder) {
                                return candidate;
                            }
                            return existing;
                        });
                    }
                }

                Files.createDirectories(targetDir);
                Map<String, Integer> destinationCounters = new HashMap<>();
                try (var paths = Files.list(sourceDir)) {
                    paths.filter(Files::isRegularFile).forEach(path -> {
                        String filename = path.getFileName().toString();
                        FaceItem item = imageToItem.get(filename);
                        if (item == null) {
                            return;
                        }
                        String ext = "";
                        int idx = filename.lastIndexOf('.');
                        if (idx >= 0) {
                            ext = filename.substring(idx);
                        }
                        Path listDir = targetDir.resolve(String.valueOf(item.listId));
                        String destinationKey = item.listId + ":" + item.id + ":" + ext.toLowerCase(Locale.ROOT);
                        int seq = destinationCounters.getOrDefault(destinationKey, 0) + 1;
                        destinationCounters.put(destinationKey, seq);
                        String destinationName = seq == 1 ? (item.id + ext) : (item.id + "_" + seq + ext);
                        Path destination = listDir.resolve(destinationName);
                        try {
                            Files.createDirectories(listDir);
                            Files.move(path, destination, StandardCopyOption.REPLACE_EXISTING);
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    });
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Face image rename failed", e);
        }
    }

    private String resolveFaceImageSelectQuery(Connection target) throws SQLException {
        if (!tableExists(target, "face_list_items")) {
            return null;
        }

        List<String> faceItemColumns = tableColumns(target, "face_list_items");
        boolean hasFaceItemsImage = faceItemColumns.contains("image");

        if (tableExists(target, "face_list_items_images")) {
            List<String> imageColumns = tableColumns(target, "face_list_items_images");
            if (imageColumns.contains("path") && imageColumns.contains("list_item_id")) {
                String imageRowsSql = "SELECT i.id, i.list_id, m.path AS image, m.id AS image_order " +
                        "FROM face_list_items i " +
                        "JOIN face_list_items_images m ON m.list_item_id = i.id " +
                        "WHERE NULLIF(TRIM(m.path), '') IS NOT NULL";

                if (hasFaceItemsImage) {
                    return "SELECT i.id, i.list_id, i.image AS image, 0 AS image_order " +
                            "FROM face_list_items i " +
                            "WHERE NULLIF(TRIM(i.image), '') IS NOT NULL " +
                            "UNION ALL " + imageRowsSql + " AND NULLIF(TRIM(i.image), '') IS NULL";
                }

                return imageRowsSql;
            }
        }

        if (hasFaceItemsImage) {
            return "SELECT id, list_id, image, 0 AS image_order FROM face_list_items";
        }

        return null;
    }

    private String extractFileName(String rawPath) {
        String normalized = rawPath.replace('\\', '/').trim();
        int slashIndex = normalized.lastIndexOf('/');
        if (slashIndex >= 0 && slashIndex + 1 < normalized.length()) {
            return normalized.substring(slashIndex + 1);
        }
        return normalized;
    }

    private Object normalizeValue(String table, String column, Object value) {
        if (value == null) {
            if (table.equals("alpr_list_items") && column.equals("created_by")) {
                return 1;
            }
            return null;
        }
        if (value instanceof String text) {
            String trimmed = text.trim();
            if (trimmed.isEmpty() || trimmed.equals("-") || trimmed.equalsIgnoreCase("NULL")) {
                return table.equals("alpr_list_items") && column.equals("created_by") ? 1 : null;
            }
            if (SENSITIVE.matcher(column).find()) {
                return trimmed;
            }
            return normalizeAscii(trimmed);
        }
        return value;
    }

    private String normalizeAscii(String text) {
        String normalized = Normalizer.normalize(text, Normalizer.Form.NFKD)
                .replace("ß", "ss")
                .replace("æ", "ae");
        return normalized.replaceAll("\\p{M}", "");
    }

    private boolean tableExists(Connection connection, String table) throws SQLException {
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getTables(null, null, table, new String[]{"TABLE"})) {
            return rs.next();
        }
    }

    private List<String> tableColumns(Connection connection, String table) throws SQLException {
        List<String> columns = new ArrayList<>();
        DatabaseMetaData meta = connection.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, table, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME").toLowerCase(Locale.ROOT));
            }
        }
        return columns;
    }

    private record MigrationConfig(String sourceType,
                                   String dumpPath,
                                   String targetType,
                                   String targetJdbcUrl,
                                   String targetUser,
                                   String targetPassword,
                                   String imageSourceDir,
                                   String imageTargetDir) {
    }

    private record FaceItem(long id, long listId, String image, int imageOrder) {
    }
}
