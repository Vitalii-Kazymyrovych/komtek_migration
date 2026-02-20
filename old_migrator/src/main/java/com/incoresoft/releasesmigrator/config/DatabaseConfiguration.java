package com.incoresoft.releasesmigrator.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.incoresoft.releasesmigrator.dto.ConfigDTO;
import com.incoresoft.releasesmigrator.dto.DatabaseConfigDTO;
import com.mysql.cj.jdbc.MysqlDataSource;
import lombok.extern.slf4j.Slf4j;
import org.jooq.ConnectionProvider;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.conf.ExecuteWithoutWhere;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;
import org.jooq.impl.DataSourceConnectionProvider;
import org.jooq.impl.DefaultConfiguration;
import org.jooq.impl.ThreadLocalTransactionProvider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import java.io.File;
import java.io.IOException;

import javax.sql.DataSource;

import static com.fasterxml.jackson.databind.PropertyNamingStrategies.SNAKE_CASE;

@Slf4j
@Configuration
public class DatabaseConfiguration {
    @Bean
    public ObjectMapper getObjectMapper() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return objectMapper.setPropertyNamingStrategy(SNAKE_CASE);
    }

    @Bean
    public DatabaseConfigDTO databaseConfigDTO(ObjectMapper objectMapper) throws IOException {
        File file = new File("config.json");
        DatabaseConfigDTO dbConfig = objectMapper.readValue(file, ConfigDTO.class).db();
        if (dbConfig == null) {
            JsonNode root = objectMapper.readTree(file);
            JsonNode target = root.path("target");
            String type = target.path("type").asText();
            String jdbcUrl = target.path("jdbc_url").asText();
            String user = target.path("user").asText();
            String password = target.path("password").asText();

            if (!jdbcUrl.isBlank()) {
                dbConfig = new DatabaseConfigDTO(jdbcUrl, password, type, user);
            }
        }

        if (dbConfig == null) {
            throw new IllegalArgumentException("Database config is missing. Expected either 'db' or 'target' section in config.json.");
        }

        if (!"mysql".equalsIgnoreCase(dbConfig.type()) && !"postgres".equalsIgnoreCase(dbConfig.type()) && !"postgresql".equalsIgnoreCase(dbConfig.type())) {
            throw new IllegalArgumentException("Unsupported database type: " + dbConfig.type());
        }

        return dbConfig;
    }

    @Bean
    public DSLContext context(DatabaseConfigDTO dbConfig) {
        Settings settings =  new Settings()
                .withRenderCatalog(false)
                .withRenderSchema(false)
                .withExecuteUpdateWithoutWhere(ExecuteWithoutWhere.LOG_INFO)
                .withExecuteDeleteWithoutWhere(ExecuteWithoutWhere.LOG_INFO);

        DataSource dataSource;
        SQLDialect sqlDialect;
        if ("mysql".equalsIgnoreCase(dbConfig.type())) {
            MysqlDataSource mysqlDataSource = new MysqlDataSource();
            mysqlDataSource.setUrl(dbConfig.host());
            mysqlDataSource.setUser(dbConfig.user());
            mysqlDataSource.setPassword(dbConfig.password());
            dataSource = mysqlDataSource;
            sqlDialect = SQLDialect.MYSQL;
        } else {
            DriverManagerDataSource postgresDataSource = new DriverManagerDataSource();
            postgresDataSource.setDriverClassName("org.postgresql.Driver");
            postgresDataSource.setUrl(dbConfig.host());
            postgresDataSource.setUsername(dbConfig.user());
            postgresDataSource.setPassword(dbConfig.password());
            dataSource = postgresDataSource;
            sqlDialect = SQLDialect.POSTGRES;
        }

        final ConnectionProvider cp = new DataSourceConnectionProvider(dataSource);

        var configuration = new DefaultConfiguration()
                .set(settings)
                .set(cp)
                .set(sqlDialect)
                .set(new ThreadLocalTransactionProvider(cp, true));

        return DSL.using(configuration);
    }
}
