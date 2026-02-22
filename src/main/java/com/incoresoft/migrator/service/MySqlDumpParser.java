package com.incoresoft.migrator.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class MySqlDumpParser {
    public record InsertStatement(String table, List<String> columns, List<List<String>> rows) {}

    public void parse(List<Path> dumpFiles, Map<String, List<String>> fallbackColumns, Consumer<InsertStatement> consumer) throws IOException {
        StringBuilder current = new StringBuilder();
        boolean inString = false;
        boolean escaped = false;

        for (Path file : dumpFiles) {
            try (BufferedReader reader = Files.newBufferedReader(file)) {
                int ch;
                while ((ch = reader.read()) != -1) {
                    char c = (char) ch;
                    current.append(c);
                    if (inString) {
                        if (escaped) escaped = false;
                        else if (c == '\\') escaped = true;
                        else if (c == '\'') inString = false;
                    } else if (c == '\'') inString = true;
                    else if (c == ';') {
                        processStatement(current.toString(), fallbackColumns, consumer);
                        current.setLength(0);
                    }
                }
            }
        }
    }

    private void processStatement(String statement, Map<String, List<String>> fallbackColumns, Consumer<InsertStatement> consumer) {
        String trimmed = statement.trim();
        if (!trimmed.toUpperCase().startsWith("INSERT INTO")) return;

        int firstTick = trimmed.indexOf('`');
        int secondTick = trimmed.indexOf('`', firstTick + 1);
        if (firstTick < 0 || secondTick < 0) return;
        String table = trimmed.substring(firstTick + 1, secondTick);

        int valuesIdx = trimmed.toUpperCase().indexOf("VALUES", secondTick);
        if (valuesIdx < 0) return;

        List<String> columns;
        int openCols = trimmed.indexOf('(', secondTick);
        if (openCols >= 0 && openCols < valuesIdx) {
            int closeCols = trimmed.indexOf(')', openCols);
            columns = parseColumns(trimmed.substring(openCols + 1, closeCols));
        } else {
            columns = fallbackColumns.get(table);
        }
        if (columns == null || columns.isEmpty()) return;

        String valuesPart = trimmed.substring(valuesIdx + 6).trim();
        if (valuesPart.endsWith(";")) valuesPart = valuesPart.substring(0, valuesPart.length() - 1);
        List<List<String>> rows = parseRows(valuesPart);
        consumer.accept(new InsertStatement(table, columns, rows));
    }

    private List<String> parseColumns(String cols) {
        List<String> out = new ArrayList<>();
        for (String c : cols.split(",")) out.add(c.replace("`", "").trim());
        return out;
    }

    private List<List<String>> parseRows(String valuesPart) {
        List<List<String>> rows = new ArrayList<>();
        int i = 0;
        while (i < valuesPart.length()) {
            while (i < valuesPart.length() && valuesPart.charAt(i) != '(') i++;
            if (i >= valuesPart.length()) break;
            i++;
            List<String> row = new ArrayList<>();
            StringBuilder token = new StringBuilder();
            boolean inString = false;
            boolean escaped = false;
            while (i < valuesPart.length()) {
                char c = valuesPart.charAt(i);
                if (inString) {
                    if (escaped) {
                        token.append(c);
                        escaped = false;
                    } else if (c == '\\') escaped = true;
                    else if (c == '\'') inString = false;
                    else token.append(c);
                    i++;
                    continue;
                }
                if (c == '\'') inString = true;
                else if (c == ',') {
                    row.add(normalize(token.toString().trim()));
                    token.setLength(0);
                } else if (c == ')') {
                    row.add(normalize(token.toString().trim()));
                    rows.add(row);
                    i++;
                    break;
                } else token.append(c);
                i++;
            }
            while (i < valuesPart.length() && (valuesPart.charAt(i) == ',' || Character.isWhitespace(valuesPart.charAt(i)))) i++;
        }
        return rows;
    }

    private String normalize(String raw) {
        if (raw.equalsIgnoreCase("NULL")) return null;
        if (raw.startsWith("_binary")) return null;
        if (raw.startsWith("b'") && raw.endsWith("'")) {
            String bit = raw.substring(2, raw.length() - 1);
            return "1".equals(bit) ? "true" : "false";
        }
        return raw;
    }
}
