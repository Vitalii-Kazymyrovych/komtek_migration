package com.incoresoft.releasesmigrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.constraint;

@Slf4j
@Component
@RequiredArgsConstructor
public class SettingsMigrator extends Migrator {
    private static final Table<?> SETTINGS = DSL.table("settings");
    private static final Field<?> SETTINGS_VARIABLE_NAME = DSL.field("Variable_name");
    private static final Field<?> SETTINGS_VARIABLE_VALUE = DSL.field("Value");

    private static final String SETTINGS_METADATA_CLEANING_CATEGORY_VARIABLE_NAME = "metadata_history_days";
    private static final String SETTINGS_ALERTS_CLEANING_CATEGORY_VARIABLE_NAME = "alerts_history_days";
    private static final String SETTINGS_IMAGE_CLEANING_CATEGORY_VARIABLE_NAME = "image_history_days";

    private static final Table<?> SYSTEM_SETTINGS = DSL.table("system_settings");
    private static final Field<?> SYSTEM_SETTINGS_VARIABLE_NAME = DSL.field("variable_name");
    private static final Field<?> SYSTEM_SETTINGS_VALUE = DSL.field("value");

    private static final Table<?> CLEANING_SETTINGS = DSL.table("cleaning_settings");
    private static final Field<?> CLEANING_SETTINGS_CATEGORY_ID = DSL.field("category_id");
    private static final Field<?> CLEANING_SETTINGS_RETENTION_PERIOD = DSL.field("retention_period");

    private final DSLContext dslContext;

    @Override
    public void processMigration() {
        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            createSystemSettingsTable(transactionContext);
            createCleaningSettingsTable(transactionContext);

            Map<String, String> settingsMap = getSettingsMap(transactionContext);

            populateSystemSettingsTable(settingsMap, transactionContext);
            populateCleaningSettingsTable(settingsMap, transactionContext);

            transactionContext.dropTable(SETTINGS).execute();
        });
    }

    private Map<String, String> getSettingsMap(DSLContext context) {
        var result = context.select(SETTINGS_VARIABLE_NAME, SETTINGS_VARIABLE_VALUE)
                .from(SETTINGS)
                .fetch();

        return result.stream()
                .collect(Collectors.toMap(
                        record -> record.get(SETTINGS_VARIABLE_NAME, String.class),
                        record -> record.get(SETTINGS_VARIABLE_VALUE, String.class)
                ));
    }

    private void createSystemSettingsTable(DSLContext context) {
        context.createTable(SYSTEM_SETTINGS)
                .column("id", SQLDataType.INTEGER.identity(true))   // AUTO_INCREMENT
                .column(SYSTEM_SETTINGS_VARIABLE_NAME, SQLDataType.VARCHAR(45).nullable(false))
                .column(SYSTEM_SETTINGS_VALUE, SQLDataType.CLOB) // TEXT maps to CLOB in jOOQ standard types
                .constraints(
                        constraint("pk_system_settings").primaryKey("id")
                )
                .execute();
        log.info("'system_settings' table created");
    }

    private void populateSystemSettingsTable(Map<String, String> settingsMap, DSLContext context) {
        for (Map.Entry<String, String> entry : settingsMap.entrySet()) {
            String variableName = entry.getKey();
            String value = entry.getValue();

            if (variableName.equals(SETTINGS_METADATA_CLEANING_CATEGORY_VARIABLE_NAME) ||
                variableName.equals(SETTINGS_ALERTS_CLEANING_CATEGORY_VARIABLE_NAME) ||
                variableName.equals(SETTINGS_IMAGE_CLEANING_CATEGORY_VARIABLE_NAME)) {
                // Skip cleaning category settings
                continue;
            }

            context.insertInto(SYSTEM_SETTINGS)
                    .columns(SYSTEM_SETTINGS_VARIABLE_NAME, SYSTEM_SETTINGS_VALUE)
                    .values(List.of(variableName, value))
                    .execute();

            log.info("'system_settings' table populated with variable {} and value {}", variableName, value);
        }
    }

    private void createCleaningSettingsTable(DSLContext context) {
        context.createTable(CLEANING_SETTINGS)
                .column(CLEANING_SETTINGS_CATEGORY_ID, SQLDataType.VARCHAR(255).nullable(false))
                .column(CLEANING_SETTINGS_RETENTION_PERIOD, SQLDataType.INTEGER.nullable(false))
                .constraints(
                        constraint("category_id").unique(CLEANING_SETTINGS_CATEGORY_ID)
                )
                .execute();
        log.info("'cleaning_settings' table created");
    }

    private void populateCleaningSettingsTable(Map<String, String> settingsMap, DSLContext context) {
        int metadataRetentionPeriod = Integer.parseInt(settingsMap.getOrDefault(SETTINGS_METADATA_CLEANING_CATEGORY_VARIABLE_NAME, "30"));
        int alertsRetentionPeriod = Integer.parseInt(settingsMap.getOrDefault(SETTINGS_ALERTS_CLEANING_CATEGORY_VARIABLE_NAME, "30"));
        int storageDataRetentionPeriod = Integer.parseInt(settingsMap.getOrDefault(SETTINGS_IMAGE_CLEANING_CATEGORY_VARIABLE_NAME, "30"));

        context.insertInto(CLEANING_SETTINGS)
                .columns(CLEANING_SETTINGS_CATEGORY_ID, CLEANING_SETTINGS_RETENTION_PERIOD)
                .values(List.of("metadata", metadataRetentionPeriod))
                .values(List.of("alerts", alertsRetentionPeriod))
                .values(List.of("storage_data", storageDataRetentionPeriod))
                .execute();

        log.info("'cleaning_settings' table populated category metadata with retention period {} days", metadataRetentionPeriod);
        log.info("'cleaning_settings' table populated category alerts with retention period {} days", alertsRetentionPeriod);
        log.info("'cleaning_settings' table populated category storage_data with retention period {} days", storageDataRetentionPeriod);
    }
}
