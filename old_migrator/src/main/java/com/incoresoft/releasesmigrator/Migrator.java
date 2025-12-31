package com.incoresoft.releasesmigrator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public abstract class Migrator {
    public void migrate() {
        String migratorName = getClass().getSimpleName();
        log.info("Starting migration for {}", migratorName);

        try {
            processMigration();
            log.info("Migration for {} completed successfully", migratorName);
        } catch (RuntimeException e) {
            log.error("Migration for {} failed", migratorName, e);
            throw e;
        }
    }

    protected abstract void processMigration();
}
