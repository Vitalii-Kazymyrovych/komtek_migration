package com.incoresoft.releasesmigrator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MigratorsManagerTest {

    private final MigratorsManager manager = new MigratorsManager(new ObjectMapper());

    @Test
    void sanitizeDumpStatementRemovesMySqlSuffixesAndSkipsLockStatements() throws Exception {
        String createSql = "CREATE TABLE users (id BIGINT) ENGINE=InnoDB AUTO_INCREMENT=42 DEFAULT CHARSET=utf8mb4";
        String sanitized = invokeStringMethod("sanitizeDumpStatement", createSql);

        assertEquals("CREATE TABLE users (id BIGINT)", sanitized);
        assertEquals("", invokeStringMethod("sanitizeDumpStatement", "LOCK TABLES users WRITE"));
        assertEquals("", invokeStringMethod("sanitizeDumpStatement", "DELIMITER $$"));
    }

    @Test
    void resolveFaceImageSelectQueryPrefersCombinedQueryWhenBothSourcesExist() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:face_query_full;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE", "sa", "");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE face_list_items (id BIGINT PRIMARY KEY, list_id BIGINT, image VARCHAR(255))");
            statement.execute("CREATE TABLE face_list_items_images (id BIGINT PRIMARY KEY, list_item_id BIGINT, path VARCHAR(255))");

            String query = invokeFaceQuery(connection);

            assertTrue(query.contains("UNION ALL"));
            assertTrue(query.contains("face_list_items_images"));
            assertTrue(query.contains("NULLIF(TRIM(i.image), '') IS NULL"));
        }
    }

    @Test
    void resolveFaceImageSelectQueryReturnsNullWhenNoImageColumnsAvailable() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:face_query_none;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE", "sa", "");
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE face_list_items (id BIGINT PRIMARY KEY, list_id BIGINT)");
            statement.execute("CREATE TABLE face_list_items_images (id BIGINT PRIMARY KEY, list_item_id BIGINT)");

            assertNull(invokeFaceQuery(connection));
        }
    }

    private String invokeStringMethod(String methodName, String input) throws Exception {
        Method method = MigratorsManager.class.getDeclaredMethod(methodName, String.class);
        method.setAccessible(true);
        return (String) method.invoke(manager, input);
    }

    private String invokeFaceQuery(Connection connection) throws Exception {
        Method method = MigratorsManager.class.getDeclaredMethod("resolveFaceImageSelectQuery", Connection.class);
        method.setAccessible(true);
        return (String) method.invoke(manager, connection);
    }
}
