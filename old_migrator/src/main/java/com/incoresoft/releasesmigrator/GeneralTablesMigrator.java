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
import java.net.URISyntaxException;
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
    private static final Set<String> IGNORED_TABLES = Set.of("face_list_items", "face_list_items_images");

    @Override
    public void migrate() {
        MigrationConfig config = loadConfig();
        DumpData dumpData = DumpParser.parseDump(Path.of(config.getSource().getDumpPath()));
        LinkedHashMap<String, TableMapping> mappingConfig = loadMappingConfig();
        JdbcTemplate targetJdbc = new JdbcTemplate(buildTargetDataSource(config));
        TransactionTemplate tx = new TransactionTemplate(new org.springframework.jdbc.datasource.DataSourceTransactionManager(buildTargetDataSource(config)));

        Map<String, LookupIndex> lookupIndexes = buildLookupIndexes(dumpData.rowsByTable());

        for (Map.Entry<String, TableMapping> entry : mappingConfig.entrySet()) {
            String tableKey = entry.getKey();
            TableMapping tableMapping = entry.getValue();
            if ("skip".equalsIgnoreCase(String.valueOf(tableMapping.action())) || IGNORED_TABLES.contains(tableKey)) {
                continue;
            }

            String sourceTable = tableMapping.sourceTable();
            String targetTable = tableMapping.targetTable();
            if (blank(targetTable)) {
                log.info("Skipping {} because target table is empty", tableKey);
                continue;
            }

            List<DumpParser.Row> rows = dumpData.rowsByTable().getOrDefault(sourceTable, List.of());
            if (rows.isEmpty()) {
                continue;
            }
            tx.execute(status -> {
                List<DumpParser.Row> prepared = prepareRows(tableMapping, rows, lookupIndexes);
                if (!prepared.isEmpty()) {
                    insertRows(targetJdbc, config.getTarget().getType(), targetTable, prepared);
                    syncSequence(targetJdbc, config.getTarget().getType(), targetTable);
                }
                return null;
            });
            log.info("Migrated table {} (source={}) -> {} rows: {}", tableKey, sourceTable, targetTable, rows.size());
        }
    }

    private List<DumpParser.Row> prepareRows(TableMapping mapping, List<DumpParser.Row> rows, Map<String, LookupIndex> lookupIndexes) {
        List<DumpParser.Row> out = new ArrayList<>();
        int rowNumber = 0;
        for (DumpParser.Row row : rows) {
            rowNumber++;
            Map<String, Object> sourceValues = new LinkedHashMap<>(row.values());
            if (isExcludedByStatus(sourceValues)) {
                continue;
            }

            Map<String, Object> values = new LinkedHashMap<>();
            for (Map.Entry<String, Object> columnMappingEntry : mapping.columnMappings().entrySet()) {
                String sourceColumn = columnMappingEntry.getKey();
                Object mappingSpec = columnMappingEntry.getValue();
                Object sourceValue = sourceValues.get(sourceColumn);
                if (mappingSpec instanceof String targetColumn) {
                    values.put(targetColumn, normalizeValue(targetColumn, sourceValue));
                    continue;
                }

                if (!(mappingSpec instanceof Map<?, ?> mappedObject)) {
                    continue;
                }

                String targetColumn = asString(mappedObject.get("target"));
                if (blank(targetColumn)) {
                    continue;
                }

                Object mappedValue = mapWithLookup(sourceColumn, sourceValue, mappedObject, lookupIndexes);
                if ("users".equals(mapping.sourceTable()) && "role_id".equals(sourceColumn) && mappedValue != null) {
                    mappedValue = String.valueOf(mappedValue);
                }
                values.put(targetColumn, normalizeValue(targetColumn, mappedValue));
            }

            applyLookupEnrichment(mapping, sourceValues, values, lookupIndexes);
            applyDefaults(mapping, values);
            values.replaceAll((k, v) -> normalizeValue(k, v));
            applyRequiredDefaults(mapping.targetTable(), values, rowNumber);
            out.add(new DumpParser.Row(row.table(), values));
        }
        return out;
    }

    private Object mapWithLookup(String sourceColumn, Object sourceValue, Map<?, ?> mappedObject, Map<String, LookupIndex> lookupIndexes) {
        Object lookupObj = mappedObject.get("lookup");
        if (!(lookupObj instanceof Map<?, ?> lookupMap)) {
            return sourceValue;
        }

        String table = asString(lookupMap.get("table"));
        String sourceKey = asString(lookupMap.get("source_key"));
        String targetKey = asString(lookupMap.get("target_key"));
        if (blank(table) || blank(sourceKey) || blank(targetKey)) {
            return sourceValue;
        }

        LookupIndex index = lookupIndexes.get(table);
        if (index == null) {
            return null;
        }

        List<Object> resolved = new ArrayList<>();
        for (String token : splitTokens(sourceValue)) {
            resolved.addAll(index.lookup(sourceKey, token, targetKey));
        }

        if (resolved.isEmpty()) {
            return null;
        }

        if (resolved.size() == 1) {
            return resolved.getFirst();
        }

        LinkedHashSet<String> merged = resolved.stream()
                .filter(Objects::nonNull)
                .map(String::valueOf)
                .collect(Collectors.toCollection(LinkedHashSet::new));
        return merged.isEmpty() ? null : String.join(",", merged);
    }

    private void applyDefaults(TableMapping mapping, Map<String, Object> values) {
        for (Map.Entry<String, Object> defaultEntry : mapping.defaults().entrySet()) {
            String targetColumn = defaultEntry.getKey();
            if (!values.containsKey(targetColumn) || values.get(targetColumn) == null) {
                values.put(targetColumn, defaultEntry.getValue());
            }
        }
    }

    private void applyLookupEnrichment(TableMapping mapping,
                                       Map<String, Object> sourceValues,
                                       Map<String, Object> targetValues,
                                       Map<String, LookupIndex> lookupIndexes) {
        for (Map<String, Object> lookup : mapping.lookups()) {
            String sourceTable = asString(lookup.get("source_table"));
            LookupIndex lookupIndex = lookupIndexes.get(sourceTable);
            if (lookupIndex == null) {
                continue;
            }

            Map<String, String> joins = castStringMap(lookup.get("join_on"));
            if (joins.isEmpty()) {
                continue;
            }

            for (Map.Entry<String, String> join : joins.entrySet()) {
                String left = join.getKey();
                String right = join.getValue();
                String sourceColumn = left.substring(left.indexOf('.') + 1);
                String lookupColumn = right.substring(right.indexOf('.') + 1);
                Object joinValue = sourceValues.get(sourceColumn);
                Map<String, Object> matched = lookupIndex.lookupFirstRow(lookupColumn, joinValue);
                if (matched == null) {
                    continue;
                }
                Map<String, String> targetFields = castStringMap(lookup.get("target_fields"));
                for (Map.Entry<String, String> field : targetFields.entrySet()) {
                    Object value = matched.get(field.getValue());
                    if (!targetValues.containsKey(field.getKey()) || targetValues.get(field.getKey()) == null) {
                        targetValues.put(field.getKey(), value);
                    }
                }
            }
        }
    }

    private List<String> splitTokens(Object sourceValue) {
        if (sourceValue == null) {
            return List.of();
        }
        String raw = String.valueOf(sourceValue);
        if (raw.isBlank()) {
            return List.of();
        }
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(token -> !token.isEmpty())
                .toList();
    }

    private LinkedHashMap<String, TableMapping> loadMappingConfig() {
        Path path = resolveMappingConfigPath();
        try {
            return new ObjectMapper().readValue(path.toFile(), new com.fasterxml.jackson.core.type.TypeReference<>() {});
        } catch (IOException e) {
            throw new RuntimeException("Cannot read mapping config from " + path, e);
        }
    }

    private Path resolveMappingConfigPath() {
        String explicitPath = System.getProperty("mapping.path", System.getenv("MIGRATOR_MAPPING_PATH"));
        if (explicitPath != null && !explicitPath.isBlank()) {
            Path explicit = Path.of(explicitPath).toAbsolutePath().normalize();
            if (!Files.exists(explicit)) {
                throw new RuntimeException("Cannot find mapping config from explicit path: " + explicit);
            }
            return explicit;
        }

        Path workingDirMapping = Path.of("mapping.json").toAbsolutePath().normalize();
        if (Files.exists(workingDirMapping)) {
            return workingDirMapping;
        }

        Path appPath;
        try {
            appPath = Path.of(GeneralTablesMigrator.class
                    .getProtectionDomain()
                    .getCodeSource()
                    .getLocation()
                    .toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException("Cannot resolve application location for mapping.json", e);
        }

        Path baseDir = Files.isDirectory(appPath) ? appPath : appPath.getParent();
        if (baseDir == null) {
            throw new RuntimeException("Cannot determine application directory for mapping.json lookup");
        }

        Path appDirMapping = baseDir.resolve("mapping.json").toAbsolutePath().normalize();
        if (Files.exists(appDirMapping)) {
            return appDirMapping;
        }

        throw new RuntimeException("Cannot find mapping config. Checked: explicit mapping.path/MIGRATOR_MAPPING_PATH, working directory "
                + workingDirMapping + ", and application directory " + appDirMapping);
    }

    private Map<String, LookupIndex> buildLookupIndexes(Map<String, List<DumpParser.Row>> rowsByTable) {
        Map<String, LookupIndex> out = new HashMap<>();
        for (Map.Entry<String, List<DumpParser.Row>> entry : rowsByTable.entrySet()) {
            out.put(entry.getKey(), new LookupIndex(entry.getValue()));
        }
        return out;
    }

    private String asString(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> castStringMap(Object value) {
        if (!(value instanceof Map<?, ?> source)) {
            return Map.of();
        }
        Map<String, String> out = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (entry.getKey() != null && entry.getValue() != null) {
                out.put(String.valueOf(entry.getKey()), String.valueOf(entry.getValue()));
            }
        }
        return out;
    }


    private void applyRequiredDefaults(String table, Map<String, Object> values, int rowNumber) {
        if ("face_lists".equals(table)) {
            if (blank(values.get("name"))) {
                values.put("name", "Unnamed face list " + rowNumber);
            }
            if (blank(values.get("color"))) {
                values.put("color", "#FFFFFF");
            }
            if (blank(values.get("list_permissions"))) {
                values.put("list_permissions", "{}");
            }
            return;
        }

        if ("streams".equals(table) && values.get("address") == null) {
            values.put("address", "");
            return;
        }

        if ("users".equals(table)) {
            if (values.get("email") == null) values.put("email", "");
            if (values.get("fullname") == null) values.put("fullname", "");
            if (values.get("password") == null) values.put("password", "");
            if (values.get("type") == null) values.put("type", "basic");
            if (values.get("ip_params") == null) values.put("ip_params", "{}");
            if (values.get("role_ids") == null) values.put("role_ids", "[]");
        }
    }

    private void insertRows(JdbcTemplate jdbcTemplate, String targetType, String table, List<DumpParser.Row> rows) {
        List<String> columns = new ArrayList<>(rows.getFirst().values().keySet());
        int chunkSize = 200;
        for (int i = 0; i < rows.size(); i += chunkSize) {
            List<DumpParser.Row> chunk = rows.subList(i, Math.min(i + chunkSize, rows.size()));
            String placeholders = "(" + String.join(",", Collections.nCopies(columns.size(), "?")) + ")";
            String sql = "INSERT INTO " + table + " (" + String.join(",", columns) + ") VALUES " +
                    chunk.stream().map(r -> placeholders).collect(Collectors.joining(",")) + duplicateClause(targetType, columns);

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

    private String duplicateClause(String targetType, List<String> columns) {
        if ("postgres".equalsIgnoreCase(targetType)) {
            return " ON CONFLICT DO NOTHING";
        }
        String noOpColumn = columns.getFirst();
        return " ON DUPLICATE KEY UPDATE " + noOpColumn + "=" + noOpColumn;
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

    private record TableMapping(@com.fasterxml.jackson.annotation.JsonProperty("source_table") String sourceTable,
                                @com.fasterxml.jackson.annotation.JsonProperty("target_table") String targetTable,
                                @com.fasterxml.jackson.annotation.JsonProperty("action") String action,
                                @com.fasterxml.jackson.annotation.JsonProperty("column_mappings") LinkedHashMap<String, Object> columnMappings,
                                @com.fasterxml.jackson.annotation.JsonProperty("defaults") LinkedHashMap<String, Object> defaults,
                                @com.fasterxml.jackson.annotation.JsonProperty("lookups") List<Map<String, Object>> lookups) {
        TableMapping {
            if (columnMappings == null) {
                columnMappings = new LinkedHashMap<>();
            }
            if (defaults == null) {
                defaults = new LinkedHashMap<>();
            }
            if (lookups == null) {
                lookups = List.of();
            }
        }
    }

    private static final class LookupIndex {
        private final List<Map<String, Object>> rows;

        private LookupIndex(List<DumpParser.Row> rows) {
            this.rows = rows.stream().map(DumpParser.Row::values).toList();
        }

        private List<Object> lookup(String sourceKey, String sourceValue, String targetKey) {
            if (sourceValue == null) {
                return List.of();
            }
            List<Object> out = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                Object candidate = row.get(sourceKey);
                if (candidate != null && sourceValue.equals(String.valueOf(candidate).trim())) {
                    out.add(row.get(targetKey));
                }
            }
            return out;
        }

        private Map<String, Object> lookupFirstRow(String sourceKey, Object sourceValue) {
            if (sourceValue == null) {
                return null;
            }
            String keyValue = String.valueOf(sourceValue).trim();
            for (Map<String, Object> row : rows) {
                Object candidate = row.get(sourceKey);
                if (candidate != null && keyValue.equals(String.valueOf(candidate).trim())) {
                    return row;
                }
            }
            return null;
        }
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
