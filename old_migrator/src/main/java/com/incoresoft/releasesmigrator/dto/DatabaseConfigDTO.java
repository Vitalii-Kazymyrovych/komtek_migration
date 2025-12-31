package com.incoresoft.releasesmigrator.dto;

public record DatabaseConfigDTO(
        String host,
        String password,
        String type,
        String user
) {
}
