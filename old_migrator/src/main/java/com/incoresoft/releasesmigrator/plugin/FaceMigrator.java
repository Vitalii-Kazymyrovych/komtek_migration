package com.incoresoft.releasesmigrator.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.releasesmigrator.AnalyticsMigrator;
import com.incoresoft.releasesmigrator.InformationSchemaManager;
import com.incoresoft.releasesmigrator.JavaObjectSerializer;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;

@Slf4j
@Component
public class FaceMigrator extends PluginMigrator{
    private static final Table<?> FACE_LISTS = DSL.table("face_lists");
    private static final Field<Integer> FACE_LISTS_ID = DSL.field(FACE_LISTS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> FACE_LISTS_STREAMS = DSL.field(FACE_LISTS.getQualifiedName().append("streams"), String.class);
    private static final Field<String> FACE_LISTS_ANALYTICS_IDS = DSL.field(FACE_LISTS.getQualifiedName().append("analytics_ids"), String.class);

    private static final Table<?> FACE_LIST_ITEMS = DSL.table("face_list_items");
    private static final Field<Integer> FACE_LIST_ITEMS_ID = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> FACE_LIST_ITEMS_EXPIRATION_SETTINGS = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("expiration_settings"), String.class);
    private static final Field<Boolean> FACE_LIST_ITEMS_EXPIRATION_SETTINGS_ENABLED = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("expiration_settings_enabled"), SQLDataType.BIT.nullable(false).defaultValue(false));
    private static final Field<String> FACE_LIST_ITEMS_EXPIRATION_SETTINGS_ACTION = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("expiration_settings_action"), SQLDataType.VARCHAR(32).nullable(false).defaultValue("none"));
    private static final Field<Integer> FACE_LIST_ITEMS_EXPIRATION_SETTINGS_LIST_ID = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("expiration_settings_list_id"), SQLDataType.INTEGER.nullable(true));
    private static final Field<Timestamp> FACE_LIST_ITEMS_EXPIRATION_SETTINGS_DATE = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("expiration_settings_date"), SQLDataType.TIMESTAMP(3).nullable(true));
    private static final Field<String> FACE_LIST_ITEMS_EXPIRATION_SETTINGS_EVENTS_HOLDER = DSL.field(FACE_LIST_ITEMS.getQualifiedName().append("expiration_settings_events_holder"), SQLDataType.CLOB.nullable(true));

    private static final Table<?> FACE_DETECTIONS = DSL.table("face_detections");
    private static final Field<Long> FACE_DETECTIONS_ID = DSL.field(FACE_DETECTIONS.getQualifiedName().append("id"), Long.class);
    private static final Field<String> FACE_DETECTIONS_UUID = DSL.field(FACE_DETECTIONS.getQualifiedName().append("uuid"), SQLDataType.CLOB.nullable(true));
    private static final Field<?> FACE_DETECTIONS_BOX = DSL.field(FACE_DETECTIONS.getQualifiedName().append("box"));
    private static final Field<String> FACE_DETECTIONS_BOX_TEMP = DSL.field(FACE_DETECTIONS.getQualifiedName().append("box_temp"), SQLDataType.CLOB.nullable(true));

    private final InformationSchemaManager informationSchemaManager;

    public FaceMigrator(AnalyticsMigrator analyticsMigrator,
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
        if (!informationSchemaManager.tableExists(FACE_DETECTIONS.getName())) {
            log.info("No '{}' table found, skipping migration for plugin face", FACE_DETECTIONS.getName());
            return;
        }
        replaceStreamIdsWithAnalyticsIdsInLists(FACE_LISTS, FACE_LISTS_ID, FACE_LISTS_STREAMS, FACE_LISTS_ANALYTICS_IDS);
        changeFormatOfDetectionBox();
        addUuidToDetection();
        migrateJsonExpirationSettingsToSeparateTables();
    }

    /**
     * Is not wrapped in transaction, because of big number of detection and possible exceeding of transaction size.
     */
    private void changeFormatOfDetectionBox() {
        dslContext.alterTable(FACE_DETECTIONS).add(FACE_DETECTIONS_BOX_TEMP).execute();

        var idAndBoxRecords = dslContext.select(FACE_DETECTIONS_ID, FACE_DETECTIONS_BOX).from(FACE_DETECTIONS).fetch();

        for (var idAndBoxRecord : idAndBoxRecords) {
            try {
                long id = idAndBoxRecord.get(FACE_DETECTIONS_ID, Long.class);
                byte[] box = idAndBoxRecord.get(FACE_DETECTIONS_BOX, byte[].class);

                Object boxObject = JavaObjectSerializer.getObject(box);

                if (boxObject instanceof double[]) {
                    setFaceDetectionsBoxTemp(id, objectMapper.writeValueAsString(boxObject));
                } else {
                    setFaceDetectionsBoxTemp(id, "[0.1, 0.1, 0.9, 0.9]");
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        dslContext.alterTable(FACE_DETECTIONS).dropColumn(FACE_DETECTIONS_BOX).execute();
        dslContext.alterTable(FACE_DETECTIONS).renameColumn(FACE_DETECTIONS_BOX_TEMP).to(FACE_DETECTIONS_BOX).execute();

        log.info("Format of data in 'box' column of 'face_detections' table is changed");
    }
    
    private void addUuidToDetection() {
        dslContext.transaction(configuration -> {
            DSLContext transactionalContext = DSL.using(configuration);

            transactionalContext.alterTable(FACE_DETECTIONS).add(FACE_DETECTIONS_UUID).execute();

            Table<?> sub = DSL
                    .select(FACE_DETECTIONS_ID, DSL.field("UUID()").as("new_uuid"))
                    .from(FACE_DETECTIONS)
                    .asTable("t");

            Field<String> NEW_UUID = sub.field("new_uuid", String.class);
            Field<Long> SUB_ID = sub.field(FACE_DETECTIONS_ID);

            transactionalContext.update(FACE_DETECTIONS.join(sub).on(FACE_DETECTIONS_ID.eq(SUB_ID)))
                    .set(FACE_DETECTIONS_UUID, NEW_UUID)
                    .execute();
        });
        log.info("UUID is added to 'face_detections' table");
    }

    private void setFaceDetectionsBoxTemp(long id, String box) {
        dslContext.update(FACE_DETECTIONS)
                .set(FACE_DETECTIONS_BOX_TEMP, box)
                .where(FACE_DETECTIONS_ID.eq(id))
                .execute();
    }

    private void migrateJsonExpirationSettingsToSeparateTables() {
        var listItemIdAndSettingsList = dslContext.select(FACE_LIST_ITEMS_ID, FACE_LIST_ITEMS_EXPIRATION_SETTINGS)
                .from(FACE_LIST_ITEMS)
                .where(FACE_LIST_ITEMS_EXPIRATION_SETTINGS.isNotNull())
                .fetch();

        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            transactionContext.alterTable(FACE_LIST_ITEMS).addColumn(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_ENABLED).execute();
            transactionContext.alterTable(FACE_LIST_ITEMS).addColumn(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_ACTION).execute();
            transactionContext.alterTable(FACE_LIST_ITEMS).addColumn(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_LIST_ID).execute();
            transactionContext.alterTable(FACE_LIST_ITEMS).addColumn(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_DATE).execute();
            transactionContext.alterTable(FACE_LIST_ITEMS).addColumn(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_EVENTS_HOLDER).execute();

            for (var listItemIdAndSettings : listItemIdAndSettingsList) {

                var listItemId = listItemIdAndSettings.get(FACE_LIST_ITEMS_ID, Integer.class);
                var expirationSettings = objectMapper.readTree(listItemIdAndSettings.get(FACE_LIST_ITEMS_EXPIRATION_SETTINGS, String.class));

                boolean enabled = expirationSettings.get("enabled") != null
                        && !expirationSettings.get("enabled").isNull() && expirationSettings.get("enabled").asBoolean();
                String action = expirationSettings.get("action").asText();
                long expiresAt = expirationSettings.get("expires_at") == null || expirationSettings.get("expires_at").isNull()
                        ? 0
                        : expirationSettings.get("expires_at").asLong();
                Integer listId = expirationSettings.get("list_id") == null || expirationSettings.get("list_id").isNull()
                        ? null
                        : expirationSettings.get("list_id").asInt();
                String eventsHolder = expirationSettings.get("events_holder") == null || expirationSettings.get("events_holder").isNull()
                        ? null
                        : expirationSettings.get("events_holder").toString();

                transactionContext.update(FACE_LIST_ITEMS)
                        .set(FACE_LIST_ITEMS_EXPIRATION_SETTINGS, (String) null)
                        .set(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_ENABLED, enabled)
                        .set(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_ACTION, action)
                        .set(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_DATE, "none".equals(action) ? null : new Timestamp(expiresAt))
                        .set(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_LIST_ID, "move".equals(action) ? listId : null)
                        .set(FACE_LIST_ITEMS_EXPIRATION_SETTINGS_EVENTS_HOLDER, eventsHolder)
                        .where(FACE_LIST_ITEMS_ID.eq(listItemId))
                        .execute();
            }

            transactionContext.createIndex("IDX__face_list_items__expiration_settings_date")
                    .on(FACE_LIST_ITEMS, FACE_LIST_ITEMS_EXPIRATION_SETTINGS_DATE)
                    .execute();
        });
        log.info("Expiration settings from 'face_list_items' table are migrated to separate columns");
    }
}
