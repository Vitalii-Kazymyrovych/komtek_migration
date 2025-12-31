package com.incoresoft.releasesmigrator.config;

import com.fasterxml.jackson.databind.DeserializationFeature;
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

import java.io.File;
import java.io.IOException;

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

        if (!"mysql".equals(dbConfig.type())) {
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

        MysqlDataSource dataSource = new MysqlDataSource();
        dataSource.setUrl(dbConfig.host());
        dataSource.setUser(dbConfig.user());
        dataSource.setPassword(dbConfig.password());

        final ConnectionProvider cp = new DataSourceConnectionProvider(dataSource);

        var configuration = new DefaultConfiguration()
                .set(settings)
                .set(cp)
                .set(SQLDialect.MYSQL)
                .set(new ThreadLocalTransactionProvider(cp, true));

        return DSL.using(configuration);
    }
}
