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

import java.util.List;

@Slf4j
@Component
public class AlprMigrator extends PluginMigrator {
    private static final Table<?> ALPR_LISTS = DSL.table("alpr_lists");
    private static final Field<Integer> ALPR_LISTS_ID = DSL.field(ALPR_LISTS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> ALPR_LISTS_STREAMS = DSL.field(ALPR_LISTS.getQualifiedName().append("streams"), String.class);
    private static final Field<String> ALPR_LISTS_ANALYTICS_IDS = DSL.field(ALPR_LISTS.getQualifiedName().append("analytics_ids"), String.class);

    private static final Table<?> ALPR_SPEED_RULES = DSL.table("alpr_speed_rules");
    private static final Field<Integer> ALPR_SPEED_RULES_ID = DSL.field(ALPR_SPEED_RULES.getQualifiedName().append("id"), Integer.class);
    private static final Field<Integer> ALPR_SPEED_RULES_STREAM_ID1 = DSL.field(ALPR_SPEED_RULES.getQualifiedName().append("stream_id1"), Integer.class);
    private static final Field<Integer> ALPR_SPEED_RULES_STREAM_ID2 = DSL.field(ALPR_SPEED_RULES.getQualifiedName().append("stream_id2"), Integer.class);
    private static final Field<Integer> ALPR_SPEED_RULES_ANALYTICS_ID1 = DSL.field(ALPR_SPEED_RULES.getQualifiedName().append("analytics_id1"), Integer.class);
    private static final Field<Integer> ALPR_SPEED_RULES_ANALYTICS_ID2 = DSL.field(ALPR_SPEED_RULES.getQualifiedName().append("analytics_id2"), Integer.class);

    private static final Table<?> ALPR_STATS_HOURLY = DSL.table("alpr_stats_hourly");
    private static final Field<Integer> ALPR_STATS_HOURLY_STREAM_ID = DSL.field(ALPR_STATS_HOURLY.getQualifiedName().append("stream_id"), Integer.class);
    private static final Field<Integer> ALPR_STATS_HOURLY_ANALYTICS_ID = DSL.field(ALPR_STATS_HOURLY.getQualifiedName().append("analytics_id"), Integer.class);

    private final InformationSchemaManager informationSchemaManager;

    public AlprMigrator(AnalyticsMigrator analyticsMigrator,
                        DSLContext dslContext,
                        ObjectMapper objectMapper,
                        InformationSchemaManager informationSchemaManager) {
        super(analyticsMigrator, dslContext, objectMapper);
        this.informationSchemaManager = informationSchemaManager;
    }

    @Override
    public String getPluginName() {
        return "alpr";
    }

    @Override
    public void processMigration() {
        if (!informationSchemaManager.tableExists(ALPR_LISTS.getName())) {
            log.info("No '{}' table found, skipping migration for plugin alpr", ALPR_LISTS.getName());
            return;
        }

        replaceStreamIdsWithAnalyticsIdsInLists(ALPR_LISTS, ALPR_LISTS_ID, ALPR_LISTS_STREAMS, ALPR_LISTS_ANALYTICS_IDS);
        replaceStreamIdsWithAnalyticsIdsInSpeedRules();
        replaceStreamIdWithAnalyticsIdInStatsHourly();
    }

    private void replaceStreamIdsWithAnalyticsIdsInSpeedRules() {
        var streamIdToAnalyticsIdMap = analyticsMigrator.getStreamIdToAnalyticsIdMap(getPluginName());

        var idAndStreamsRecords = dslContext.select(ALPR_SPEED_RULES_ID, ALPR_SPEED_RULES_STREAM_ID1, ALPR_SPEED_RULES_STREAM_ID2)
                .from(ALPR_SPEED_RULES)
                .fetch();

        dslContext.transaction(configuration -> {
            DSLContext transactionContext = configuration.dsl();

            for (var idAndStreamsRecord : idAndStreamsRecords) {
                int speedRuleId = idAndStreamsRecord.get(ALPR_SPEED_RULES_ID, Integer.class);
                int streamId1 = idAndStreamsRecord.get(ALPR_SPEED_RULES_STREAM_ID1, Integer.class);
                Integer streamId2 = idAndStreamsRecord.get(ALPR_SPEED_RULES_STREAM_ID2, Integer.class);

                Integer analyticsId1 = streamIdToAnalyticsIdMap.getOrDefault(streamId1, -1);
                Integer analyticsId2 = streamIdToAnalyticsIdMap.getOrDefault(streamId2, -1);

                transactionContext.update(ALPR_SPEED_RULES)
                        .set(ALPR_SPEED_RULES_STREAM_ID1, analyticsId1)
                        .set(ALPR_SPEED_RULES_STREAM_ID2, analyticsId2)
                        .where(ALPR_SPEED_RULES_ID.eq(speedRuleId))
                        .execute();
            }

            transactionContext.alterTable(ALPR_SPEED_RULES).renameColumn(ALPR_SPEED_RULES_STREAM_ID1).to(ALPR_SPEED_RULES_ANALYTICS_ID1).execute();
            transactionContext.alterTable(ALPR_SPEED_RULES).renameColumn(ALPR_SPEED_RULES_STREAM_ID2).to(ALPR_SPEED_RULES_ANALYTICS_ID2).execute();
        });

        log.info("Replaced stream ids with analytics ids in 'alpr_speed_rules' table");
    }

    /**
     * Is not wrapped in transaction, because of big number of detection and possible exceeding of transaction size.
     */
    private void replaceStreamIdWithAnalyticsIdInStatsHourly() {
        var streamIdToAnalyticsIdMap = analyticsMigrator.getStreamIdToAnalyticsIdMap(getPluginName());

        List<Integer> nonExistentStreamIds = dslContext.selectDistinct(ALPR_STATS_HOURLY_STREAM_ID)
                .from(ALPR_STATS_HOURLY)
                .where(ALPR_STATS_HOURLY_STREAM_ID.notIn(streamIdToAnalyticsIdMap.keySet()))
                .fetch(record -> record.get(ALPR_STATS_HOURLY_STREAM_ID));

        for (var entry : streamIdToAnalyticsIdMap.entrySet()) {
            dslContext.update(ALPR_STATS_HOURLY)
                    .set(ALPR_STATS_HOURLY_STREAM_ID, entry.getValue())
                    .where(ALPR_STATS_HOURLY_STREAM_ID.eq(entry.getKey()))
                    .execute();
        }
        dslContext.update(ALPR_STATS_HOURLY)
                .set(ALPR_STATS_HOURLY_STREAM_ID, -1)
                .where(ALPR_STATS_HOURLY_STREAM_ID.in(nonExistentStreamIds))
                .execute();

        dslContext.alterTable(ALPR_STATS_HOURLY).renameColumn(ALPR_STATS_HOURLY_STREAM_ID).to(ALPR_STATS_HOURLY_ANALYTICS_ID).execute();

        log.info("Replaced stream id with analytics id in 'alpr_stats_hourly' table");
    }
}
