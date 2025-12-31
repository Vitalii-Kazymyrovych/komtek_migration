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
public class SmartVaMigrator extends PluginMigrator {
    private static final Table<?> SMART_VA_LISTS = DSL.table("smart_va_lists");
    private static final Field<Integer> SMART_VA_LISTS_ID = DSL.field(SMART_VA_LISTS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> SMART_VA_LISTS_STREAMS = DSL.field(SMART_VA_LISTS.getQualifiedName().append("streams"), String.class);
    private static final Field<String> SMART_VA_LISTS_ANALYTICS = DSL.field(SMART_VA_LISTS.getQualifiedName().append("analytics"), String.class);

    private static final Table<?> SMART_VA_NOTIFICATIONS = DSL.table("smart_va_notifications");
    private static final Field<Integer> SMART_VA_NOTIFICATIONS_ID = DSL.field(SMART_VA_NOTIFICATIONS.getQualifiedName().append("id"), Integer.class);
    private static final Field<String> SMART_VA_NOTIFICATION_PRESENCE_ACTION_TYPE = DSL.field(SMART_VA_NOTIFICATIONS.getQualifiedName().append("action_type"), SQLDataType.CLOB.nullable(true));

    private static final Table<?> NOTIFICATION_PRESENCE_ACTION_TYPES = DSL.table("notifications_presence_action_types");
    private static final Field<Integer> NOTIFICATION_PRESENCE_ACTION_TYPES_NOTIFICATION_ID = DSL.field(NOTIFICATION_PRESENCE_ACTION_TYPES.getQualifiedName().append("notification_id"), Integer.class);
    private static final Field<String> NOTIFICATION_PRESENCE_ACTION_TYPES_ACTION_TYPE = DSL.field(NOTIFICATION_PRESENCE_ACTION_TYPES.getQualifiedName().append("action_type"), SQLDataType.CLOB.nullable(true));

    private final InformationSchemaManager informationSchemaManager;

    public SmartVaMigrator(AnalyticsMigrator analyticsMigrator,
                           DSLContext dslContext,
                           ObjectMapper objectMapper,
                           InformationSchemaManager informationSchemaManager) {
        super(analyticsMigrator, dslContext, objectMapper);
        this.informationSchemaManager = informationSchemaManager;
    }

    @Override
    public String getPluginName() {
        return "smart_va";
    }

    @Override
    protected void processMigration() {
        if (!informationSchemaManager.tableExists(SMART_VA_LISTS.getName())) {
            log.info("No '{}' table found, skipping migration for plugin smart va", SMART_VA_LISTS.getName());
            return;
        }

        replaceStreamIdsWithAnalyticsIdsInLists(SMART_VA_LISTS, SMART_VA_LISTS_ID, SMART_VA_LISTS_STREAMS, SMART_VA_LISTS_ANALYTICS);
        refactorNotificationPresence();
    }

    private void refactorNotificationPresence() {
        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            transactionContext.alterTable(SMART_VA_NOTIFICATIONS).addColumn(SMART_VA_NOTIFICATION_PRESENCE_ACTION_TYPE).execute();

            transactionContext.update(SMART_VA_NOTIFICATIONS.innerJoin(NOTIFICATION_PRESENCE_ACTION_TYPES).on(SMART_VA_NOTIFICATIONS_ID.eq(NOTIFICATION_PRESENCE_ACTION_TYPES_NOTIFICATION_ID)))
                    .set(SMART_VA_NOTIFICATION_PRESENCE_ACTION_TYPE, NOTIFICATION_PRESENCE_ACTION_TYPES_ACTION_TYPE)
                    .execute();

            transactionContext.dropTable(NOTIFICATION_PRESENCE_ACTION_TYPES).execute();
        });

        log.info("Refactored notification presence: dropped table 'notification_presence_action_types' and added column 'action_type' to table 'smart_va_notifications'");
    }
}
