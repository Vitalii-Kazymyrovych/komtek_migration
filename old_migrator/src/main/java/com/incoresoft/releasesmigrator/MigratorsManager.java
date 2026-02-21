package com.incoresoft.releasesmigrator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class MigratorsManager {
    private final List<Migrator> migrators;

    public void migrate() {
        for (Migrator migrator : migrators) {
            log.info("Running migrator: {}", migrator.getClass().getSimpleName());
            migrator.migrate();
        }
    }
}
