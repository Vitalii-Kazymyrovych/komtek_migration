package com.incoresoft.migrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.migrator.model.Config;
import com.incoresoft.migrator.model.MappingSpec;

import java.nio.file.Path;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class MigrationService {
    private static final Set<String> TABLES_TO_MIGRATE = Set.of(
            "analytics", "clients", "event_manager", "face_lists", "roles", "servers", "settings",
            "stats_traffic_minutely", "stream_groups", "streams", "traffic_stat", "traffic_counters", "users");

    public void migrate(Path basePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        Config config = mapper.readValue(basePath.resolve("config.json").toFile(), Config.class);
        MappingSpec mapping = mapper.readValue(basePath.resolve("mapping.json").toFile(), MappingSpec.class);

        String pgUrl = "jdbc:postgresql://%s:%d/%s?stringtype=unspecified".formatted(config.postgres.host, config.postgres.port, config.postgres.database);
        Class.forName("org.h2.Driver");
        Class.forName("org.postgresql.Driver");
        try (Connection h2 = DriverManager.getConnection("jdbc:h2:mem:mig;MODE=MySQL;DATABASE_TO_UPPER=false;DB_CLOSE_DELAY=-1");
             Connection pg = DriverManager.getConnection(pgUrl, config.postgres.user, config.postgres.password)) {
            pg.setSchema(config.postgres.schema);
            stageToH2(h2, config, basePath);
            migrateToPostgres(h2, pg, mapping, config.postgres.schema);
        }
    }

    private void stageToH2(Connection h2, Config config, Path basePath) throws Exception {
        Map<String, List<String>> tableColumns = parseOldDdlColumns(basePath.resolve("oldDDL.sql"));
        MySqlDumpParser parser = new MySqlDumpParser();
        List<Path> files = config.dumpFiles.stream().map(basePath::resolve).toList();

        parser.parse(files, tableColumns, insert -> {
            if (!TABLES_TO_MIGRATE.contains(insert.table())) return;
            try {
                createStagingTable(h2, insert.table(), insert.columns());
                insertRows(h2, insert.table(), insert.columns(), insert.rows());
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private Map<String, List<String>> parseOldDdlColumns(Path oldDdl) throws java.io.IOException {
        String sql = java.nio.file.Files.readString(oldDdl);
        Map<String, List<String>> out = new HashMap<>();
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("CREATE TABLE `([^`]+)` \\((.*?)\\) ENGINE=", java.util.regex.Pattern.DOTALL).matcher(sql);
        while (m.find()) {
            List<String> cols = new ArrayList<>();
            for (String line : m.group(2).split("\n")) {
                java.util.regex.Matcher cm = java.util.regex.Pattern.compile("^\\s*`([^`]+)`\\s+").matcher(line);
                if (cm.find()) cols.add(cm.group(1));
            }
            out.put(m.group(1), cols);
        }
        return out;
    }

    private void createStagingTable(Connection h2, String table, List<String> columns) throws SQLException {
        String ddl = "CREATE TABLE IF NOT EXISTS " + q(table) + " (" +
                columns.stream().map(c -> q(c) + " CLOB").collect(Collectors.joining(",")) + ")";
        try (Statement st = h2.createStatement()) {
            st.execute(ddl);
        }
    }

    private void insertRows(Connection h2, String table, List<String> columns, List<List<String>> rows) throws SQLException {
        String sql = "INSERT INTO " + q(table) + " (" + columns.stream().map(this::q).collect(Collectors.joining(",")) + ") VALUES (" +
                columns.stream().map(c -> "?").collect(Collectors.joining(",")) + ")";
        try (PreparedStatement ps = h2.prepareStatement(sql)) {
            for (List<String> row : rows) {
                for (int i = 0; i < columns.size(); i++) ps.setString(i + 1, i < row.size() ? row.get(i) : null);
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private void migrateToPostgres(Connection h2, Connection pg, MappingSpec mapping, String schema) throws SQLException {
        Map<String, Map<String, String>> lookups = new HashMap<>();
        for (MappingSpec.TableMapping table : mapping.tables) {
            if (table.sourceTable.equals("traffic_counters")) continue;
            if (!TABLES_TO_MIGRATE.contains(table.sourceTable) || !hasTable(h2, table.sourceTable)) continue;
            try (Statement clean = pg.createStatement()) {
                clean.execute("TRUNCATE TABLE " + schema + "." + table.targetTable + " CASCADE");
            }

            List<Map<String, String>> sourceRows = readAll(h2, table.sourceTable);
            List<MappingSpec.ColumnMapping> usable = table.columnMappings;
            if (usable.isEmpty()) continue;
            String insertSql = "INSERT INTO " + schema + "." + table.targetTable + " (" +
                    usable.stream().map(cm -> cm.target).collect(Collectors.joining(",")) + ") VALUES (" +
                    usable.stream().map(cm -> "?").collect(Collectors.joining(",")) + ")";

            try (PreparedStatement ps = pg.prepareStatement(insertSql)) {
                for (Map<String, String> row : sourceRows) {
                    for (int i = 0; i < usable.size(); i++) ps.setObject(i + 1, transform(usable.get(i), row, h2, lookups));
                    ps.addBatch();
                }
                ps.executeBatch();
            }
        }
    }

    private Object transform(MappingSpec.ColumnMapping cm, Map<String, String> row, Connection h2, Map<String, Map<String, String>> cache) {
        if ("send_internal_notifications".equals(cm.target)) {
            String v = row.get(cm.source);
            if (v == null || v.isBlank()) return false;
        }
        return switch (cm.strategy) {
            case "DIRECT", "COPY_ID" -> autoType(row.get(cm.source));
            case "CONSTANT" -> autoType(cm.value);
            case "GENERATED_UUID" -> java.util.UUID.randomUUID().toString();
            case "ROLE_TO_ARRAY" -> row.get(cm.source) == null ? "[]" : "[" + row.get(cm.source) + "]";
            case "LOOKUP" -> {
                MappingSpec.Lookup lk = cm.lookup;
                String cacheKey = lk.table + ":" + lk.lookupKey + ":" + lk.lookupValue;
                cache.computeIfAbsent(cacheKey, k -> loadLookup(h2, lk.table, lk.lookupKey, lk.lookupValue));
                yield autoType(cache.get(cacheKey).get(row.get(lk.sourceColumn)));
            }
            default -> null;
        };
    }

    private Map<String, String> loadLookup(Connection h2, String table, String key, String val) {
        Map<String, String> out = new HashMap<>();
        try (Statement st = h2.createStatement(); ResultSet rs = st.executeQuery("SELECT " + q(key) + "," + q(val) + " FROM " + q(table))) {
            while (rs.next()) out.put(rs.getString(1), rs.getString(2));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    private boolean hasTable(Connection c, String table) throws SQLException {
        try (ResultSet rs = c.getMetaData().getTables(null, null, table, null)) {
            return rs.next();
        }
    }


    private Object autoType(String v) {
        if (v == null) return null;
        if ("true".equalsIgnoreCase(v) || "false".equalsIgnoreCase(v)) return Boolean.parseBoolean(v);
        if (v.matches("[0-9a-fA-F-]{36}")) {
            try { return java.util.UUID.fromString(v); } catch (Exception ignored) {}
        }
        if (v.matches("-?\\d+")) {
            try { return Integer.parseInt(v); } catch (Exception ignored) {}
            try { return Long.parseLong(v); } catch (Exception ignored) {}
        }
        if (v.matches("-?\\d+\\.\\d+")) {
            try { return Double.parseDouble(v); } catch (Exception ignored) {}
        }
        return v;
    }

    private String q(String name) { return "\"" + name + "\""; }

    private List<Map<String, String>> readAll(Connection c, String table) throws SQLException {
        List<Map<String, String>> out = new ArrayList<>();
        try (Statement st = c.createStatement(); ResultSet rs = st.executeQuery("SELECT * FROM " + q(table))) {
            ResultSetMetaData md = rs.getMetaData();
            while (rs.next()) {
                Map<String, String> row = new HashMap<>();
                for (int i = 1; i <= md.getColumnCount(); i++) row.put(md.getColumnName(i), rs.getString(i));
                out.add(row);
            }
        }
        return out;
    }
}
