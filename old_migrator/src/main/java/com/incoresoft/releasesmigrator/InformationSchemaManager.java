package com.incoresoft.releasesmigrator;

import com.incoresoft.releasesmigrator.dto.DatabaseConfigDTO;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

import java.util.regex.Pattern;

@Component
public class InformationSchemaManager {
    private static final Pattern MYSQL_PATTERN = Pattern.compile("^jdbc:mysql://([^/]+)/([^?]+)");

    private final String schemaName;
    private final DSLContext dslContext;

    public InformationSchemaManager(DatabaseConfigDTO databaseConfigDTO, DSLContext dslContext) {
        this.dslContext = dslContext;
        this.schemaName = getSchemaName(databaseConfigDTO);
    }

    public boolean tableExists(String tableName) {
        var resultOptional = dslContext.select()
                .from("information_schema.tables")
                .where(DSL.field("table_schema", String.class).eq(DSL.inline(schemaName))
                        .and(DSL.field("table_name", String.class).eq(DSL.inline(tableName))))
                .fetchOptional();

        return resultOptional.isPresent();
    }

    private String getSchemaName(DatabaseConfigDTO databaseConfigDTO) {
        var url = databaseConfigDTO.host();
        var matcher = MYSQL_PATTERN.matcher(url);
        if (matcher.find()) {
            return matcher.group(2);
        } else {
            throw new IllegalArgumentException("Invalid JDBC URL: " + url);
        }

    }
}
