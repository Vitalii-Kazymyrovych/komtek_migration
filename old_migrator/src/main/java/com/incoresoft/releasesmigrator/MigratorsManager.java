package com.incoresoft.releasesmigrator;

import com.incoresoft.releasesmigrator.plugin.PluginMigrator;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
public class MigratorsManager {
    private final SettingsMigrator settingsMigrator;
    private final AnalyticsMigrator analyticsMigrator;
    private final UsersMigrator usersMigrator;
    private final List<PluginMigrator> pluginMigrators;
    private final DatabaseChangelogMigrator databaseChangelogMigrator;

    public void migrate() {
        settingsMigrator.migrate();
        analyticsMigrator.migrate();
        usersMigrator.migrate();

        for (PluginMigrator pluginMigrator : pluginMigrators) {
            pluginMigrator.migrate();
        }

        databaseChangelogMigrator.migrate();
    }
}
