package com.incoresoft.releasesmigrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.incoresoft.releasesmigrator.dto.AdvancedSettingsImpl;
import com.incoresoft.releasesmigrator.dto.LprAdvancedSettings;
import com.incoresoft.releasesmigrator.dto.NewSmokeFireHardhatAnalyticsModule;
import com.incoresoft.releasesmigrator.dto.OldSmokeFireHardhatAnalyticsModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.constraint;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnalyticsMigrator extends Migrator {
    private static final Table<?> ANALYTICS = DSL.table("analytics");
    private static final Field<Integer> ANALYTICS_ID = DSL.field(ANALYTICS.getQualifiedName().append("id"), Integer.class);
    private static final Field<Integer> ANALYTICS_STREAM_ID = DSL.field(ANALYTICS.getQualifiedName().append("stream_id"), Integer.class);
    private static final Field<String> ANALYTICS_STREAM_UUID = DSL.field(ANALYTICS.getQualifiedName().append("stream_uuid"), String.class);
    private static final Field<Integer> ANALYTICS_GROUP_ID = DSL.field(ANALYTICS.getQualifiedName().append("group_id"), SQLDataType.INTEGER.defaultValue(0).nullable(false));
    private static final Field<String> ANALYTICS_PLUGIN_NAME = DSL.field(ANALYTICS.getQualifiedName().append("plugin_name"), String.class);
    private static final Field<String> ANALYTICS_MODULE = DSL.field(ANALYTICS.getQualifiedName().append("module"), String.class);

    private static final Table<?> STREAMS = DSL.table("streams");
    private static final Field<Integer> STREAMS_ID = DSL.field(STREAMS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> STREAMS_UUID = DSL.field(STREAMS.getQualifiedName().append("uuid"), String.class);
    private static final Field<Integer> STREAMS_PARENT_ID = DSL.field(STREAMS.getQualifiedName().append("parent_id"), Integer.class);

    private static final Table<?> STREAM_GROUPS = DSL.table("stream_groups");
    private static final Field<Integer> STREAM_GROUPS_ID = DSL.field(STREAM_GROUPS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> STREAM_GROUPS_NAME = DSL.field(STREAM_GROUPS.getQualifiedName().append("name"), String.class);
    private static final Field<Integer> STREAM_GROUPS_PARENT_ID = DSL.field(STREAM_GROUPS.getQualifiedName().append("parent_id"), Integer.class);
    private static final Field<Integer> STREAM_GROUPS_CLIENT_ID = DSL.field(STREAM_GROUPS.getQualifiedName().append("client_id"), Integer.class);

    private static final Table<?> ANALYTICS_GROUPS = DSL.table("analytics_groups");
    private static final Field<Integer> ANALYTICS_GROUPS_ID = DSL.field(ANALYTICS_GROUPS.getQualifiedName().append("id"), SQLDataType.INTEGER.identity(true).nullable(false));
    private static final Field<String> ANALYTICS_GROUPS_NAME = DSL.field(ANALYTICS_GROUPS.getQualifiedName().append("name"), SQLDataType.CLOB.nullable(false));
    private static final Field<Integer> ANALYTICS_GROUPS_PARENT_ID = DSL.field(ANALYTICS_GROUPS.getQualifiedName().append("parent_id"), SQLDataType.INTEGER.nullable(false));
    private static final Field<Integer> ANALYTICS_GROUPS_CLIENT_ID = DSL.field(ANALYTICS_GROUPS.getQualifiedName().append("client_id"), SQLDataType.INTEGER.nullable(false));
    private static final Field<String> ANALYTICS_GROUPS_PLUGIN_NAME = DSL.field(ANALYTICS_GROUPS.getQualifiedName().append("plugin_name"), SQLDataType.CLOB.nullable(false));

    private final DSLContext dslContext;
    private final ObjectMapper objectMapper;

    /**
     * Get a map of stream IDs to analytics IDs for a given plugin name.
     * In general, multiple analytics can be created for a single stream.
     * This method will return the first analytics ID for each stream ID.
     * <p>
     * {@code Important:} This method need to be executed after {@code stream_id} is replaced with
     * {@code stream_uuid} in {@code analytics} table
     *
     * @param pluginName                plugin name
     * @return                          map of stream IDs to analytics IDs
     */
    public Map<Integer, Integer> getStreamIdToAnalyticsIdMap(String pluginName) {
        Map<Integer, Integer> map = new HashMap<>();

        var streamIdAndAnalyticsIdRecords = dslContext.select(STREAMS_ID, ANALYTICS_ID)
                .from(STREAMS.innerJoin(ANALYTICS).on(STREAMS_UUID.eq(ANALYTICS_STREAM_UUID)))
                .where(ANALYTICS_PLUGIN_NAME.eq(pluginName))
                .fetch();

        for (var record : streamIdAndAnalyticsIdRecords) {
            Integer streamId = record.get(STREAMS_ID);
            Integer analyticsId = record.get(ANALYTICS_ID);

            if (map.containsKey(streamId) || analyticsId == null) {
                continue;
            }
            map.put(streamId, analyticsId);
        }

        return Map.copyOf(map);
    }

    @Override
    public void processMigration() {
        addGroupId();
        replaceStreamIdWithStreamUuid();

        updateLprModules();
        updateSmokefireAndHardhatModules();
        updateGeneralModules();
    }

    private void replaceStreamIdWithStreamUuid() {
        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            transactionContext.alterTable(ANALYTICS).addColumn(ANALYTICS_STREAM_UUID, SQLDataType.CHAR(36)).execute();

            transactionContext.update(ANALYTICS)
                    .set(ANALYTICS_STREAM_UUID,
                            transactionContext.select(STREAMS_UUID)
                                    .from(STREAMS)
                                    .where(STREAMS_ID.eq(ANALYTICS_STREAM_ID))
                                    .asField())
                    .execute();

            transactionContext.alterTable(ANALYTICS)
                    .dropColumn(ANALYTICS_STREAM_ID)
                    .execute();
        });

        log.info("Stream ID column replaced with Stream UUID in 'analytics' table");
    }

    private void addGroupId() {
        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            addGroupIdField(transactionContext);

            createAnalyticsGroupsTable(transactionContext);

            var analyticsRecords = transactionContext.select(ANALYTICS_ID, ANALYTICS_STREAM_ID, ANALYTICS_PLUGIN_NAME).from(ANALYTICS).fetch();

            for (var analyticsRecord : analyticsRecords) {
                int analyticsId = analyticsRecord.get(ANALYTICS_ID);
                int streamId = analyticsRecord.get(ANALYTICS_STREAM_ID);
                String pluginName = analyticsRecord.get(ANALYTICS_PLUGIN_NAME);

                var streamParentIdOptional = getStreamParentId(streamId, transactionContext);

                if (streamParentIdOptional.isEmpty()) {
                    continue;
                }

                var streamGroupRecordOptional = transactionContext.select(STREAM_GROUPS_ID, STREAM_GROUPS_NAME, STREAM_GROUPS_PARENT_ID, STREAM_GROUPS_CLIENT_ID)
                        .from(STREAM_GROUPS)
                        .where(STREAM_GROUPS_ID.eq(streamParentIdOptional.get()))
                        .fetchOptional();

                if (streamGroupRecordOptional.isEmpty()) {
                    continue;
                }

                String groupName = streamGroupRecordOptional.get().get(STREAM_GROUPS_NAME);
                int groupParentId = streamGroupRecordOptional.get().get(STREAM_GROUPS_PARENT_ID);
                int groupClientId = streamGroupRecordOptional.get().get(STREAM_GROUPS_CLIENT_ID);

                var analyticsGroupIdOptional = getAnalyticsGroupId(groupName, pluginName, groupClientId, transactionContext);

                if (analyticsGroupIdOptional.isEmpty()) {
                    int groupId = createAnalyticsGroup(groupName, groupParentId, pluginName, groupClientId, transactionContext);
                    updateAnalyticsGroupId(analyticsId, groupId, transactionContext);
                } else {
                    updateAnalyticsGroupId(analyticsId, analyticsGroupIdOptional.get(), transactionContext);
                }
            }
        });
    }

    private int createAnalyticsGroup(String name, int parentId, String pluginName, int clientId, DSLContext context) {
        context.insertInto(ANALYTICS_GROUPS)
                .columns(ANALYTICS_GROUPS_NAME, ANALYTICS_GROUPS_PARENT_ID, ANALYTICS_GROUPS_PLUGIN_NAME, ANALYTICS_GROUPS_CLIENT_ID)
                .values(name, parentId, pluginName, clientId)
                .execute();

        var id = context.select(ANALYTICS_GROUPS_ID)
                .from(ANALYTICS_GROUPS)
                .where(ANALYTICS_GROUPS_NAME.eq(name)
                        .and(ANALYTICS_GROUPS_CLIENT_ID.eq(clientId))
                        .and(ANALYTICS_GROUPS_PLUGIN_NAME.eq(pluginName))
                )
                .fetchOne()
                .get(ANALYTICS_GROUPS_ID);

        log.info("Analytics group created with id: {}, name: {}, parentId: {}, pluginName: {}, clientId: {}", id, name, parentId, pluginName, clientId);
        return id;
    }

    private Optional<Integer> getStreamParentId(int streamId, DSLContext context) {
        return context.select(STREAMS_PARENT_ID)
                .from(STREAMS)
                .where(STREAMS_ID.eq(streamId))
                .fetchOptional()
                .map(record -> record.get(STREAMS_PARENT_ID));
    }

    private Optional<Integer> getAnalyticsGroupId(String name, String pluginName, int clientId, DSLContext context) {
        return context.select(ANALYTICS_GROUPS_ID)
                .from(ANALYTICS_GROUPS)
                .where(ANALYTICS_GROUPS_NAME.eq(name)
                        .and(ANALYTICS_GROUPS_CLIENT_ID.eq(clientId))
                        .and(ANALYTICS_GROUPS_PLUGIN_NAME.eq(pluginName))
                )
                .fetchOptional()
                .map(record -> record.get(ANALYTICS_GROUPS_ID));
    }

    private void createAnalyticsGroupsTable(DSLContext context) {
        context.createTable(ANALYTICS_GROUPS)
                .column(ANALYTICS_GROUPS_ID)
                .column(ANALYTICS_GROUPS_NAME)
                .column(ANALYTICS_GROUPS_PARENT_ID)
                .column(ANALYTICS_GROUPS_PLUGIN_NAME)
                .column(ANALYTICS_GROUPS_CLIENT_ID)
                .constraints(constraint("pk_analytics_groups").primaryKey("id"))
                .execute();

        log.info("'analytics_groups' table created");
    }

    private void addGroupIdField(DSLContext context) {
        context.alterTable(ANALYTICS).add(ANALYTICS_GROUP_ID).execute();

        log.info("Group ID column added to 'analytics' table");
    }

    private void updateAnalyticsGroupId(int analyticsId, int groupId, DSLContext context) {
        context.update(ANALYTICS).set(ANALYTICS_GROUP_ID, groupId).where(ANALYTICS_ID.eq(analyticsId)).execute();

        log.info("Analytics with id: {} had group id updated with value: {}", analyticsId, groupId);
    }

    private void updateLprModules() {
        var idWithModuleRecords = dslContext.select(ANALYTICS_ID, ANALYTICS_MODULE)
                .from(ANALYTICS)
                .where(ANALYTICS_PLUGIN_NAME.eq("alpr"))
                .fetch();

        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            for (var idWithModuleRecord : idWithModuleRecords) {
                try {
                    int analyticsId = idWithModuleRecord.get(ANALYTICS_ID);

                    ObjectNode oldModuleNode = (ObjectNode) objectMapper.readTree(idWithModuleRecord.get(ANALYTICS_MODULE));

                    ObjectNode updatedModuleNode = getUpdatedLprModule(oldModuleNode);

                    if (!oldModuleNode.equals(updatedModuleNode)) {
                        transactionContext.update(ANALYTICS)
                                .set(ANALYTICS_MODULE, objectMapper.writeValueAsString(updatedModuleNode))
                                .where(ANALYTICS_ID.eq(analyticsId))
                                .execute();

                        log.info("Updated module for analytics with id: {}. Old module: {}, new module: {}", analyticsId, oldModuleNode, updatedModuleNode);
                    }

                } catch (JsonProcessingException e) {
                    log.error("Error while parsing module JSON", e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private ObjectNode getUpdatedLprModule(ObjectNode moduleNode) throws JsonProcessingException {
        ObjectNode updatedModuleNode = moduleNode.deepCopy();

        JsonNode oldAdvancedSettingNode = updatedModuleNode.get("advanced_settings");
        LprAdvancedSettings oldAdvancedSettings = objectMapper.treeToValue(oldAdvancedSettingNode, LprAdvancedSettings.class);

        LprAdvancedSettings fixedAdvancedSettings = fixLprAdvancedSettings(oldAdvancedSettings);
        JsonNode fixedAdvancedSettingsNode = objectMapper.valueToTree(fixedAdvancedSettings);

        updatedModuleNode.set("advanced_settings", fixedAdvancedSettingsNode);
        return updatedModuleNode;
    }

    private LprAdvancedSettings fixLprAdvancedSettings(LprAdvancedSettings advancedSettings) {
        Set<String> COUNTRY_CODES = Arrays.stream(Locale.getISOCountries()).collect(Collectors.toSet());
        // as of JDK 21,Kosovo, East Timor, Netherlands Antilles is not included in the list of ISO countries
        Set<String> ADDITIONAL_COUNTRY_CODES = Set.of("XK", "TP", "AN");

        LprAdvancedSettings updating = advancedSettings
                .withMinPlateWidth(Math.max(advancedSettings.getMinPlateWidth(), 40))
                .withMinPlateHeight(Math.max(advancedSettings.getMinPlateHeight(), 15))
                .withFramesToDetect(Math.clamp(advancedSettings.getFramesToDetect(), 3, 1000))
                .withMinPlateLength(Math.clamp(advancedSettings.getMinPlateLength(), 1, 15))
                .withMaxPlateLength(Math.clamp(advancedSettings.getMaxPlateLength(), 1, 15))
                .withSensitivity(0.5f)
                .withCountries(
                        advancedSettings.getCountries().stream()
                                .filter(country -> COUNTRY_CODES.contains(country) || ADDITIONAL_COUNTRY_CODES.contains(country))
                                .toList()
                );

        if (updating.getMinPlateLength() > updating.getMaxPlateLength()) {
            updating = updating.withMaxPlateLength(15);
        }

        return updating;
    }

    private void updateSmokefireAndHardhatModules() {
        var idWithModuleRecords = dslContext.select(ANALYTICS_ID, ANALYTICS_MODULE)
                .from(ANALYTICS)
                .where(ANALYTICS_PLUGIN_NAME.in("hardhat", "smoke_fire"))
                .fetch();

        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            for (var idWithModuleRecord : idWithModuleRecords) {
                try {
                    int analyticsId = idWithModuleRecord.get(ANALYTICS_ID);

                    ObjectNode oldModuleNode = (ObjectNode) objectMapper.readTree(idWithModuleRecord.get(ANALYTICS_MODULE));

                    ObjectNode updatedModuleNode = getUpdatedSmokefireOrHardhatModule(oldModuleNode);

                    if (!oldModuleNode.equals(updatedModuleNode)) {
                        transactionContext.update(ANALYTICS)
                                .set(ANALYTICS_MODULE, objectMapper.writeValueAsString(updatedModuleNode))
                                .where(ANALYTICS_ID.eq(analyticsId))
                                .execute();

                        log.info("Updated module for analytics with id: {}. Old module: {}, new module: {}", analyticsId, oldModuleNode, updatedModuleNode);
                    }

                } catch (JsonProcessingException e) {
                    log.error("Error while parsing module JSON", e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private ObjectNode getUpdatedSmokefireOrHardhatModule(ObjectNode moduleNode) throws JsonProcessingException {
        OldSmokeFireHardhatAnalyticsModule oldModule = objectMapper.treeToValue(moduleNode, OldSmokeFireHardhatAnalyticsModule.class);

        NewSmokeFireHardhatAnalyticsModule newModule = new NewSmokeFireHardhatAnalyticsModule(oldModule);

        return objectMapper.valueToTree(newModule);
    }

    private void updateGeneralModules() {
        var idWithModuleRecords = dslContext.select(ANALYTICS_ID, ANALYTICS_MODULE)
                .from(ANALYTICS)
                .where(ANALYTICS_PLUGIN_NAME.in("smart_va", "object_in_zone", "traffic", "gun_detection", "military"))
                .fetch();

        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            for (var idWithModuleRecord : idWithModuleRecords) {
                try {
                    int analyticsId = idWithModuleRecord.get(ANALYTICS_ID);

                    ObjectNode oldModuleNode = (ObjectNode) objectMapper.readTree(idWithModuleRecord.get(ANALYTICS_MODULE));

                    ObjectNode updatedModuleNode = getUpdatedGeneralModule(oldModuleNode);

                    if (!oldModuleNode.equals(updatedModuleNode)) {
                    transactionContext.update(ANALYTICS)
                            .set(ANALYTICS_MODULE, objectMapper.writeValueAsString(updatedModuleNode))
                            .where(ANALYTICS_ID.eq(analyticsId))
                            .execute();

                        log.info("Updated module for analytics with id: {}. Old module: {}, new module: {}", analyticsId, oldModuleNode, updatedModuleNode);
                    }
                } catch (JsonProcessingException e) {
                    log.error("Error while parsing module JSON", e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private ObjectNode getUpdatedGeneralModule(ObjectNode moduleNode) throws JsonProcessingException {
        ObjectNode updatedModuleNode = moduleNode.deepCopy();

        JsonNode oldAdvancedSettingNode = updatedModuleNode.get("advanced_settings");
        AdvancedSettingsImpl oldAdvancedSettings = objectMapper.treeToValue(oldAdvancedSettingNode, AdvancedSettingsImpl.class);

        AdvancedSettingsImpl fixedAdvancedSettings = fixGeneralAdvancedSettings(oldAdvancedSettings);
        JsonNode fixedAdvancedSettingsNode = objectMapper.valueToTree(fixedAdvancedSettings);

        updatedModuleNode.set("advanced_settings", fixedAdvancedSettingsNode);
        return updatedModuleNode;
    }

    private AdvancedSettingsImpl fixGeneralAdvancedSettings(AdvancedSettingsImpl advancedSettings) {
        return advancedSettings
                .withSensitivity(Math.clamp(advancedSettings.getSensitivity(), 1.0f, 10.0f))
                .withTrackerBufferTime(Math.max(advancedSettings.getTrackerBufferTime(), 10))
                .withMinHeight(Math.max(advancedSettings.getMinHeight(), 25))
                .withMinWidth(Math.max(advancedSettings.getMinWidth(), 25));
    }
}
