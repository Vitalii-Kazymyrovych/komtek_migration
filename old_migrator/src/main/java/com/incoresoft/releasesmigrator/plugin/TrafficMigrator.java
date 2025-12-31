package com.incoresoft.releasesmigrator.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.releasesmigrator.AnalyticsMigrator;
import com.incoresoft.releasesmigrator.InformationSchemaManager;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TrafficMigrator extends PluginMigrator {
    private final Table<?> STATS_TRAFFIC_HOURLY = DSL.table("stats_traffic_hourly");

    private final InformationSchemaManager informationSchemaManager;

    public TrafficMigrator(AnalyticsMigrator analyticsMigrator,
                           DSLContext dslContext,
                           ObjectMapper objectMapper,
                           InformationSchemaManager informationSchemaManager) {
        super(analyticsMigrator, dslContext, objectMapper);
        this.informationSchemaManager = informationSchemaManager;
    }

    @Override
    public String getPluginName() {
        return "traffic";
    }

    @Override
    public void processMigration() {
        if (informationSchemaManager.tableExists(STATS_TRAFFIC_HOURLY.getName())) {
            dslContext.dropTable(STATS_TRAFFIC_HOURLY).execute();
            log.info("Dropped deprecated table '{}'", STATS_TRAFFIC_HOURLY.getName());
        }
    }
}
