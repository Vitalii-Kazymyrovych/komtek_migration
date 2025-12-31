package com.incoresoft.releasesmigrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@Slf4j
@SpringBootApplication
@RequiredArgsConstructor
public class ReleasesMigratorApplication implements CommandLineRunner {
    private final MigratorsManager migratorsManager;

    public static void main(String[] args) {
        SpringApplication.run(ReleasesMigratorApplication.class, args);
    }

    @Override
    public void run(String... args) {
        migratorsManager.migrate();
        log.info("Migration completed successfully.");
    }
}
