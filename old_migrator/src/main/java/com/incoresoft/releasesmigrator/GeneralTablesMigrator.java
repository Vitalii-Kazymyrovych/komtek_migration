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
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.CharsetDecoder;
import java.nio.file.*;
import java.text.Normalizer;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

        log.info("Loaded dump from {} with {} tables", config.getSource().getDumpPath(), dumpData.rowsByTable().size());
        log.info("Loaded mapping config with {} table definitions", mappingConfig.size());

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
                log.info("Skipping table {} (source={}) because source table has no rows in dump", tableKey, sourceTable);
                continue;
            }

            TableMigrationStats stats = tx.execute(status -> {
                PrepareRowsResult preparedResult = prepareRows(tableMapping, rows, lookupIndexes);
                int insertedRows = 0;
                if (!preparedResult.rows().isEmpty()) {
                    insertedRows = insertRows(targetJdbc, config.getTarget().getType(), targetTable, preparedResult.rows());
                    syncSequence(targetJdbc, config.getTarget().getType(), targetTable);
                }
                return new TableMigrationStats(rows.size(), preparedResult.rows().size(), preparedResult.excludedByStatus(), insertedRows);
            });

            if (stats == null) {
                log.warn("Migration transaction returned no stats for table {} (source={})", tableKey, sourceTable);
                continue;
            }

            log.info("Migration summary for {} (source={} -> target={}): sourceRows={}, preparedRows={}, excludedByStatus={}, insertedOrUpdatedRows={}",
                    tableKey,
                    sourceTable,
                    targetTable,
                    stats.sourceRows(),
                    stats.preparedRows(),
                    stats.excludedByStatus(),
                    stats.insertedRows());

            if (stats.preparedRows() == 0) {
                log.warn("Prepared 0 rows for table {}. Most likely all source rows were filtered out (e.g. status=-1) or mapping produced empty values.", tableKey);
            }
            if (stats.preparedRows() > 0 && stats.insertedRows() == 0) {
                log.warn("No new rows were inserted for table {} although {} rows were prepared. Existing target rows may already conflict (ON CONFLICT DO NOTHING / ON DUPLICATE KEY no-op).", tableKey, stats.preparedRows());
            }
        }
    }

    private PrepareRowsResult prepareRows(TableMapping mapping, List<DumpParser.Row> rows, Map<String, LookupIndex> lookupIndexes) {
        List<DumpParser.Row> out = new ArrayList<>();
        int excludedByStatus = 0;
        int rowNumber = 0;
        for (DumpParser.Row row : rows) {
            rowNumber++;
            Map<String, Object> sourceValues = new LinkedHashMap<>(row.values());
            if (isExcludedByStatus(sourceValues)) {
                excludedByStatus++;
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
        return new PrepareRowsResult(out, excludedByStatus);
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

    private int insertRows(JdbcTemplate jdbcTemplate, String targetType, String table, List<DumpParser.Row> rows) {
        List<String> columns = new ArrayList<>(rows.getFirst().values().keySet());
        int chunkSize = 200;
        int affectedRows = 0;
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
            int chunkAffected = jdbcTemplate.update(sql, params.toArray());
            affectedRows += Math.max(chunkAffected, 0);
            log.debug("Inserted chunk for table {}: chunkSize={}, affectedRows={}", table, chunk.size(), chunkAffected);
        }
        return affectedRows;
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

    @com.fasterxml.jackson.annotation.JsonIgnoreProperties(ignoreUnknown = true)
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

    record DumpData(Map<String, List<DumpParser.Row>> rowsByTable) {
    }

    private record PrepareRowsResult(List<DumpParser.Row> rows, int excludedByStatus) {
    }

    private record TableMigrationStats(int sourceRows, int preparedRows, int excludedByStatus, int insertedRows) {
    }

    static final class DumpParser {
        private static final Pattern INSERT_PREFIX = Pattern.compile("(?is)^\\s*(?:INSERT(?:\\s+IGNORE)?|REPLACE)\\s+INTO\\s+");

        static DumpData parseDump(Path path) {
            List<Path> dumpPaths = resolveDumpPaths(path);
            if (dumpPaths.isEmpty()) {
                throw new RuntimeException("No dump files found for path: " + path.toAbsolutePath().normalize());
            }

            Map<String, List<Row>> byTable = new LinkedHashMap<>();
            for (Path dumpPath : dumpPaths) {
                appendDump(dumpPath, byTable);
            }
            return new DumpData(byTable);
        }

        private static void appendDump(Path path, Map<String, List<Row>> byTable) {
            try {
                String sql = readDumpText(path);
                for (String statement : splitStatements(sql)) {
                    String t = statement.trim();
                    if (!isSupportedInsertStatement(t)) {
                        continue;
                    }
                    ParsedInsert parsed = parseInsert(t);
                    if (parsed == null) continue;
                    byTable.computeIfAbsent(parsed.table(), k -> new ArrayList<>()).addAll(parsed.rows());
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to parse dump: " + path, e);
            }
        }

        private static List<Path> resolveDumpPaths(Path configuredPath) {
            Path normalized = configuredPath.toAbsolutePath().normalize();
            if (Files.exists(normalized)) {
                if (Files.isDirectory(normalized)) {
                    try (var stream = Files.list(normalized)) {
                        return stream
                                .filter(Files::isRegularFile)
                                .filter(path -> path.getFileName().toString().toLowerCase(Locale.ROOT).endsWith(".sql"))
                                .sorted()
                                .toList();
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to list dump directory: " + normalized, e);
                    }
                }
                return List.of(normalized);
            }

            Path parent = normalized.getParent();
            String pattern = normalized.getFileName() == null ? "" : normalized.getFileName().toString();
            if (parent == null || !Files.exists(parent)) {
                return List.of();
            }

            try (DirectoryStream<Path> stream = Files.newDirectoryStream(parent, pattern)) {
                List<Path> matches = new ArrayList<>();
                for (Path entry : stream) {
                    if (Files.isRegularFile(entry)) {
                        matches.add(entry.toAbsolutePath().normalize());
                    }
                }
                matches.sort(Comparator.naturalOrder());
                return matches;
            } catch (IOException e) {
                throw new RuntimeException("Failed to resolve dump glob path: " + normalized, e);
            }
        }

        private static String readDumpText(Path path) throws IOException {
            byte[] bytes = Files.readAllBytes(path);
            if (bytes.length >= 2) {
                int b0 = bytes[0] & 0xFF;
                int b1 = bytes[1] & 0xFF;
                if (b0 == 0xFF && b1 == 0xFE) {
                    return stripBom(new String(bytes, StandardCharsets.UTF_16LE));
                }
                if (b0 == 0xFE && b1 == 0xFF) {
                    return stripBom(new String(bytes, StandardCharsets.UTF_16BE));
                }
            }

            String strictUtf8 = tryDecodeStrictUtf8(bytes);
            if (strictUtf8 != null) {
                return stripBom(strictUtf8);
            }

            if (looksLikeUtf16(bytes)) {
                return stripBom(new String(bytes, StandardCharsets.UTF_16LE));
            }

            return stripBom(new String(bytes, StandardCharsets.UTF_8));
        }

        private static String tryDecodeStrictUtf8(byte[] bytes) {
            CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT);
            try {
                return decoder.decode(java.nio.ByteBuffer.wrap(bytes)).toString();
            } catch (CharacterCodingException ex) {
                return null;
            }
        }

        private static boolean looksLikeUtf16(byte[] bytes) {
            if (bytes.length < 4 || bytes.length % 2 != 0) {
                return false;
            }

            int zeroEven = 0;
            int zeroOdd = 0;
            int pairs = bytes.length / 2;
            for (int i = 0; i + 1 < bytes.length; i += 2) {
                if (bytes[i] == 0) {
                    zeroEven++;
                }
                if (bytes[i + 1] == 0) {
                    zeroOdd++;
                }
            }

            double evenRatio = (double) zeroEven / pairs;
            double oddRatio = (double) zeroOdd / pairs;
            return evenRatio > 0.30 || oddRatio > 0.30;
        }

        private static String stripBom(String value) {
            if (!value.isEmpty() && value.charAt(0) == "\ufeff".charAt(0)) {
                return value.substring(1);
            }
            return value;
        }

        private static boolean isSupportedInsertStatement(String statement) {
            return INSERT_PREFIX.matcher(statement).find();
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
            Matcher matcher = INSERT_PREFIX.matcher(statement);
            if (!matcher.find()) {
                return null;
            }
            int intoIdx = matcher.end();
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
