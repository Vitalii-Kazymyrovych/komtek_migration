package com.incoresoft.releasesmigrator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.text.Normalizer;
import java.time.Instant;
import java.util.*;
import java.util.regex.Pattern;

@Slf4j
@Component
@RequiredArgsConstructor
public class MigratorsManager {
    private static final List<String> TABLE_ORDER = List.of(
            "clients",
            "roles",
            "users",
            "stream_groups",
            "streams",
            "analytics_groups",
            "analytics",
            "event_manager",
            "alpr_lists",
            "alpr_list_items",
            "face_lists"
    );
    private static final Pattern SENSITIVE = Pattern.compile("(?i)(password|hash|token|secret|signature|base64)");

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
        String dumpSql = Files.readString(Path.of(config.dumpPath), StandardCharsets.UTF_8);
        for (String statement : splitSqlStatements(cleanDumpSql(dumpSql))) {
            if (statement.isBlank()) {
                continue;
            }
            try (Statement st = connection.createStatement()) {
                st.execute(statement);
            } catch (SQLException ex) {
                log.debug("Skipped source statement: {}", ex.getMessage());
            }
        }
        return connection;
    }

    private Connection openTargetConnection(MigrationConfig config) throws Exception {
        Class.forName("org.postgresql.Driver");
        return DriverManager.getConnection(config.targetJdbcUrl, config.targetUser, config.targetPassword);
    }

    private void migrateTable(Connection source, Connection target, String table) throws SQLException {
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

        String insertSql = "INSERT INTO " + table + " (" + String.join(",", columns) + ") VALUES (" + String.join(",", Collections.nCopies(columns.size(), "?")) + ") ON CONFLICT DO NOTHING";

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
        log.info("Migrated {} rows into {}", migrated, table);
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

        try (PreparedStatement ps = target.prepareStatement("SELECT id, list_id, image FROM face_list_items");
             ResultSet rs = ps.executeQuery()) {
            Map<String, FaceItem> imageToItem = new HashMap<>();
            while (rs.next()) {
                String image = rs.getString("image");
                if (image != null && !image.isBlank()) {
                    imageToItem.put(image, new FaceItem(rs.getLong("id"), rs.getLong("list_id"), image));
                }
            }

            Files.createDirectories(targetDir);
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
                    Path destination = listDir.resolve(item.id + ext);
                    try {
                        Files.createDirectories(listDir);
                        Files.move(path, destination);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        } catch (Exception e) {
            throw new IllegalStateException("Face image rename failed", e);
        }
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

    private String cleanDumpSql(String sql) {
        return sql
                .replace("`", "")
                .replaceAll("/\\*!.*?\\*/", " ")
                .replaceAll("(?im)^\\s*LOCK TABLES.*$", "")
                .replaceAll("(?im)^\\s*UNLOCK TABLES.*$", "")
                .replaceAll("(?im)^\\s*DELIMITER.*$", "")
                .replaceAll("(?im)ENGINE\\s*=\\s*\\w+", "")
                .replaceAll("(?im)DEFAULT CHARSET\\s*=\\s*\\w+", "");
    }

    private List<String> splitSqlStatements(String sql) {
        List<String> statements = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuote = false;
        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);
            if (c == '\'' && (i == 0 || sql.charAt(i - 1) != '\\')) {
                inQuote = !inQuote;
            }
            if (c == ';' && !inQuote) {
                statements.add(current.toString().trim());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        if (!current.isEmpty()) {
            statements.add(current.toString().trim());
        }
        return statements;
    }

    private record MigrationConfig(String sourceType,
                                   String dumpPath,
                                   String targetJdbcUrl,
                                   String targetUser,
                                   String targetPassword,
                                   String imageSourceDir,
                                   String imageTargetDir) {
    }

    private record FaceItem(long id, long listId, String image) {
    }
}
