package com.incoresoft.migrator.model;

import java.util.ArrayList;
import java.util.List;

public class MappingSpec {
    public List<TableMapping> tables = new ArrayList<>();

    public static class TableMapping {
        public String sourceTable;
        public String targetTable;
        public List<ColumnMapping> columnMappings = new ArrayList<>();
        public List<String> unmappedTargetColumns = new ArrayList<>();
    }

    public static class ColumnMapping {
        public String target;
        public String strategy;
        public String source;
        public String value;
        public Lookup lookup;
    }

    public static class Lookup {
        public String table;
        public String sourceColumn;
        public String lookupKey;
        public String lookupValue;
    }
}
