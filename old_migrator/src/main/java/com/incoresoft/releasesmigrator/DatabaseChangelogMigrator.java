package com.incoresoft.releasesmigrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DatabaseChangelogMigrator extends Migrator {
    private final DSLContext dslContext;
    private final InformationSchemaManager informationSchemaManager;

    @Override
    protected void processMigration() {
        if (informationSchemaManager.tableExists("databasechangelog")) {
            dslContext.dropTable("databasechangelog").execute();
            log.info("Dropped 'databasechangelog' table");
        }
    }
}
