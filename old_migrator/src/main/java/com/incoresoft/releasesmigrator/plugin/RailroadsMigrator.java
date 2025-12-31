package com.incoresoft.releasesmigrator.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.releasesmigrator.AnalyticsMigrator;
import com.incoresoft.releasesmigrator.InformationSchemaManager;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class RailroadsMigrator extends PluginMigrator {
    private final Table<?> RAILROAD_NUMBERS = DSL.table("railroad_numbers");
    private final Field<Integer> RAILROAD_NUMBERS_STREAM_ID = DSL.field("stream_id", Integer.class);
    private final Field<Integer> RAILROAD_NUMBERS_ANALYTICS_ID = DSL.field("analytics_id", Integer.class);
    private final Field<String> RAILROAD_NUMBERS_ZONE = DSL.field("zone", SQLDataType.CLOB.nullable(true));

    private final InformationSchemaManager informationSchemaManager;

    public RailroadsMigrator(AnalyticsMigrator analyticsMigrator,
                        DSLContext dslContext,
                        ObjectMapper objectMapper,
                        InformationSchemaManager informationSchemaManager) {
        super(analyticsMigrator, dslContext, objectMapper);
        this.informationSchemaManager = informationSchemaManager;
    }

    @Override
    public String getPluginName() {
        return "railroad";
    }

    @Override
    protected void processMigration() {
        if (!informationSchemaManager.tableExists(RAILROAD_NUMBERS.getName())) {
            log.info("No '{}' table found, skipping migration for plugin railroad", RAILROAD_NUMBERS.getName());
            return;
        }
        addAnalyticsIdToNumbers();
        addZoneColumn();
    }

    /**
     * Is not wrapped in transaction, because of big number of detection and possible exceeding of transaction size.
     */
    private void addAnalyticsIdToNumbers() {
        dslContext.alterTable(RAILROAD_NUMBERS).addColumn(RAILROAD_NUMBERS_ANALYTICS_ID).execute();

        var streamIdToAnalyticsIdMap = analyticsMigrator.getStreamIdToAnalyticsIdMap(getPluginName());

        for (var entry : streamIdToAnalyticsIdMap.entrySet()) {
            var streamId = entry.getKey();
            var analyticsId = entry.getValue();

            dslContext.update(RAILROAD_NUMBERS)
                    .set(RAILROAD_NUMBERS_ANALYTICS_ID, analyticsId)
                    .where(RAILROAD_NUMBERS_STREAM_ID.eq(streamId))
                    .execute();
        }

        dslContext.update(RAILROAD_NUMBERS)
                .set(RAILROAD_NUMBERS_ANALYTICS_ID, -1)
                .where(RAILROAD_NUMBERS_STREAM_ID.notIn(streamIdToAnalyticsIdMap.keySet()))
                .execute();

        log.info("Added analytics_id to {}", RAILROAD_NUMBERS.getName());
    }

    private void addZoneColumn() {
        dslContext.alterTable(RAILROAD_NUMBERS).addColumn(RAILROAD_NUMBERS_ZONE).execute();
        log.info("Added zone column to {}", RAILROAD_NUMBERS.getName());
    }
}
