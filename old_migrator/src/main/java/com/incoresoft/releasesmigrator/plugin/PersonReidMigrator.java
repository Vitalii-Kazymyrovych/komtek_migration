package com.incoresoft.releasesmigrator.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.releasesmigrator.AnalyticsMigrator;
import com.incoresoft.releasesmigrator.InformationSchemaManager;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PersonReidMigrator extends PluginMigrator {
    private final Table<?> SMART_TRACKING_FRAME_REID = DSL.table("smart_tracking_frame_reid");
    private final Field<Integer> SMART_TRACKING_FRAME_REID_STREAM_ID = DSL.field("stream_id", Integer.class);
    private final Field<Integer> SMART_TRACKING_FRAME_REID_ANALYTICS_ID = DSL.field("analytics_id", Integer.class);

    private final InformationSchemaManager informationSchemaManager;

    public PersonReidMigrator(AnalyticsMigrator analyticsMigrator,
                             DSLContext dslContext,
                             ObjectMapper objectMapper,
                             InformationSchemaManager informationSchemaManager) {
        super(analyticsMigrator, dslContext, objectMapper);
        this.informationSchemaManager = informationSchemaManager;
    }

    @Override
    public String getPluginName() {
        return "person_reid";
    }

    @Override
    protected void processMigration() {
        if (!informationSchemaManager.tableExists(SMART_TRACKING_FRAME_REID.getName())) {
            log.info("No '{}' table found, skipping migration for plugin person reid", SMART_TRACKING_FRAME_REID.getName());
            return;
        }
        addAnalyticsIdToDetectionFrames();
    }

    /**
     * Is not wrapped in transaction, because of big number of detection and possible exceeding of transaction size.
     */
    private void addAnalyticsIdToDetectionFrames() {
        dslContext.alterTable(SMART_TRACKING_FRAME_REID).addColumn(SMART_TRACKING_FRAME_REID_ANALYTICS_ID).execute();

        var streamIdToAnalyticsIdMap = analyticsMigrator.getStreamIdToAnalyticsIdMap(getPluginName());

        for (var entry : streamIdToAnalyticsIdMap.entrySet()) {
            var streamId = entry.getKey();
            var analyticsId = entry.getValue();

            dslContext.update(SMART_TRACKING_FRAME_REID)
                    .set(SMART_TRACKING_FRAME_REID_ANALYTICS_ID, analyticsId)
                    .where(SMART_TRACKING_FRAME_REID_STREAM_ID.eq(streamId))
                    .execute();
        }

        dslContext.update(SMART_TRACKING_FRAME_REID)
                .set(SMART_TRACKING_FRAME_REID_ANALYTICS_ID, -1)
                .where(SMART_TRACKING_FRAME_REID_STREAM_ID.notIn(streamIdToAnalyticsIdMap.keySet()))
                .execute();

        log.info("Added analytics_id to {}", SMART_TRACKING_FRAME_REID.getName());
    }
}
