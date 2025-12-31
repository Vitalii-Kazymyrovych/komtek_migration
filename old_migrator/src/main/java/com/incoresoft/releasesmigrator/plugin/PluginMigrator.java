package com.incoresoft.releasesmigrator.plugin;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.releasesmigrator.AnalyticsMigrator;
import com.incoresoft.releasesmigrator.Migrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;

import java.util.List;
import java.util.Objects;

@Slf4j
@RequiredArgsConstructor
public abstract class PluginMigrator extends Migrator {
    protected final AnalyticsMigrator analyticsMigrator;
    protected final DSLContext dslContext;
    protected final ObjectMapper objectMapper;


    public abstract String getPluginName();


    protected void replaceStreamIdsWithAnalyticsIdsInLists(Table<?> listsTable,
                                                           Field<Integer> listsIdTable,
                                                           Field<String> listsStreamsTable,
                                                           Field<String> listsAnalyticsIdsTable) {
        var streamIdToAnalyticsIdMap = analyticsMigrator.getStreamIdToAnalyticsIdMap(getPluginName());

        var idAndStreamsRecords = dslContext.select(listsIdTable, listsStreamsTable).from(listsTable).fetch();

        dslContext.transaction(configuration -> {
            DSLContext transactionalContext = DSL.using(configuration);

            for (var idAndStreamsRecord : idAndStreamsRecords) {
                try {
                    int listId = idAndStreamsRecord.get(listsIdTable, Integer.class);

                    List<Integer> streamIds = objectMapper.readValue(idAndStreamsRecord.get(listsStreamsTable, String.class), new TypeReference<>() {});

                    List<Integer> analyticsIds = streamIds.stream()
                            .map(streamIdToAnalyticsIdMap::get)
                            .filter(Objects::nonNull)
                            .toList();

                    transactionalContext.update(listsTable)
                            .set(listsStreamsTable, objectMapper.writeValueAsString(analyticsIds))
                            .where(listsIdTable.eq(listId))
                            .execute();

                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }
            }

            transactionalContext.alterTable(listsTable).renameColumn(listsStreamsTable).to(listsAnalyticsIdsTable).execute();
        });

        log.info("Replaced stream ids with analytics ids in '{}' table", listsTable.getName());
    }
}
