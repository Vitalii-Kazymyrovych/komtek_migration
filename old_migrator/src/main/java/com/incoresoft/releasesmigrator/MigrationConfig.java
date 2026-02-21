package com.incoresoft.releasesmigrator;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class MigrationConfig {
    private SourceConfig source;
    private TargetConfig target;
    private ImagesConfig images;

    @Data
    public static class SourceConfig {
        private String type;
        @JsonProperty("dump_path")
        private String dumpPath;
    }

    @Data
    public static class TargetConfig {
        private String type;
        @JsonProperty("jdbc_url")
        private String jdbcUrl;
        private String user;
        private String password;
    }

    @Data
    public static class ImagesConfig {
        @JsonProperty("source_dir")
        private String sourceDir = "./face_lists";
        @JsonProperty("target_dir")
        private String targetDir = "./face_lists_new";
    }
}
