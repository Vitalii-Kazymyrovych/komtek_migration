package com.incoresoft.migrator.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.incoresoft.migrator.model.MappingSpec;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MappingGenerator {
    private static final List<String> TABLES = List.of(
            "analytics", "clients", "event_manager", "face_lists", "roles", "servers",
            "stats_traffic_minutely", "stream_groups", "streams", "traffic_stat", "traffic_counters", "users"
    );

    public static void ensureMapping(Path oldDdl, Path newDdl, Path output) throws IOException {
        MappingSpec spec = buildMapping(oldDdl, newDdl);
        ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
        mapper.writeValue(output.toFile(), spec);
    }

    private static MappingSpec buildMapping(Path oldDdl, Path newDdl) throws IOException {
        String oldSql = Files.readString(oldDdl);
        String newSql = Files.readString(newDdl);
        Map<String, List<String>> oldTables = parseOld(oldSql);
        Map<String, List<String>> newTables = parseNew(newSql);

        MappingSpec spec = new MappingSpec();
        for (String table : TABLES) {
            MappingSpec.TableMapping tm = new MappingSpec.TableMapping();
            tm.sourceTable = table;
            tm.targetTable = table.equals("settings") ? "system_settings" : table;

            List<String> srcCols = oldTables.getOrDefault(table, List.of());
            List<String> tgtCols = newTables.getOrDefault(tm.targetTable, List.of());
            Set<String> sourceSet = new HashSet<>(srcCols);

            for (String tgt : tgtCols) {
                MappingSpec.ColumnMapping cm = new MappingSpec.ColumnMapping();
                cm.target = tgt;
                if (table.equals("event_manager") && tgt.equals("id")) {
                    tm.unmappedTargetColumns.add(tgt);
                    continue;
                }
                if (sourceSet.contains(tgt)) {
                    cm.strategy = "DIRECT";
                    cm.source = tgt;
                    tm.columnMappings.add(cm);
                    continue;
                }

                if (table.equals("analytics") && tgt.equals("stream_uuid")) {
                    cm.strategy = "LOOKUP";
                    MappingSpec.Lookup lookup = new MappingSpec.Lookup();
                    lookup.table = "streams";
                    lookup.sourceColumn = "stream_id";
                    lookup.lookupKey = "id";
                    lookup.lookupValue = "uuid";
                    cm.lookup = lookup;
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("analytics") && tgt.equals("uuid")) {
                    cm.strategy = "GENERATED_UUID";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("analytics") && tgt.equals("group_id")) {
                    cm.strategy = "CONSTANT";
                    cm.value = "0";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("analytics") && tgt.equals("topic")) {
                    continue;
                }
                if (table.equals("event_manager") && tgt.equals("uuid")) {
                    cm.strategy = "COPY_ID";
                    cm.source = "id";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("users") && tgt.equals("role_ids")) {
                    cm.strategy = "ROLE_TO_ARRAY";
                    cm.source = "role_id";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("face_lists") && tgt.equals("analytics_ids")) {
                    cm.strategy = "FACE_LIST_ANALYTICS_IDS";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("settings") && tgt.equals("variable_name")) {
                    cm.strategy = "DIRECT";
                    cm.source = "Variable_name";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("settings") && tgt.equals("value")) {
                    cm.strategy = "DIRECT";
                    cm.source = "Value";
                    tm.columnMappings.add(cm);
                    continue;
                }
                if (table.equals("traffic_stat") && Set.of("x1","y1","x2","y2","confidence").contains(tgt)) {
                    cm.strategy = "CONSTANT";
                    cm.value = "0";
                    tm.columnMappings.add(cm);
                    continue;
                }

                tm.unmappedTargetColumns.add(tgt);
            }
            spec.tables.add(tm);
        }
        return spec;
    }

    private static Map<String, List<String>> parseOld(String sql) {
        Map<String, List<String>> out = new HashMap<>();
        Matcher m = Pattern.compile("CREATE TABLE `([^`]+)` \\((.*?)\\) ENGINE=", Pattern.DOTALL).matcher(sql);
        while (m.find()) {
            List<String> cols = new ArrayList<>();
            for (String line : m.group(2).split("\n")) {
                Matcher cm = Pattern.compile("\\s*`([^`]+)`\\s+").matcher(line);
                if (cm.find()) cols.add(cm.group(1));
            }
            out.put(m.group(1), cols);
        }
        return out;
    }

    private static Map<String, List<String>> parseNew(String sql) {
        Map<String, List<String>> out = new HashMap<>();
        Matcher m = Pattern.compile("CREATE TABLE\\s+[\\w]+\\.([\\w]+)\\s*\\((.*?)\\);", Pattern.DOTALL).matcher(sql);
        while (m.find()) {
            List<String> cols = new ArrayList<>();
            for (String line : m.group(2).split("\n")) {
                line = line.strip();
                if (line.isEmpty() || line.startsWith("CONSTRAINT") || line.startsWith("PRIMARY KEY")) continue;
                Matcher cm = Pattern.compile("\"?([a-zA-Z_][\\w]*)\"?\\s+").matcher(line);
                if (cm.find()) cols.add(cm.group(1));
            }
            out.put(m.group(1), cols);
        }
        return out;
    }
}
