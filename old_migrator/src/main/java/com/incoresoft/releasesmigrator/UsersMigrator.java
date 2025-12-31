package com.incoresoft.releasesmigrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class UsersMigrator extends Migrator {
    private static final Table<?> USERS = DSL.table("users");
    private static final Field<Integer> USERS_ROLE_ID = DSL.field("role_id", Integer.class);
    private static final Field<String> USERS_ROLE_IDS = DSL.field("role_ids", SQLDataType.CLOB);

    private final DSLContext dslContext;

    @Override
    public void processMigration() {
        dslContext.transaction(configuration -> {
            DSLContext transactionContext = DSL.using(configuration);

            transactionContext.alterTable(USERS)
                    .add(USERS_ROLE_IDS)
                    .execute();

            transactionContext.update(USERS)
                    .set(USERS_ROLE_IDS, DSL.concat(DSL.inline("["), USERS_ROLE_ID, DSL.inline("]")))
                    .execute();

            transactionContext.alterTable(USERS)
                    .dropColumn(USERS_ROLE_ID)
                    .execute();

            // jooq generated code to add not null constraint does NOT work on MySQL
            transactionContext.execute("ALTER TABLE users MODIFY role_ids TEXT NOT NULL");
        });

        log.info("Role id is replaced by role ids in 'users' table");
    }
}
