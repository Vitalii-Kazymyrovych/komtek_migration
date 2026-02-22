package com.incoresoft.migrator;

import com.incoresoft.migrator.service.MappingGenerator;
import com.incoresoft.migrator.service.MigrationService;

import java.nio.file.Path;

public class MigratorApplication {
    public static void main(String[] args) throws Exception {
        Path basePath = Path.of(".").toAbsolutePath().normalize();
        MappingGenerator.ensureMapping(basePath.resolve("oldDDL.sql"), basePath.resolve("newDDL.sql"), basePath.resolve("mapping.json"));
        new MigrationService().migrate(basePath);
        System.out.println("Migration completed.");
    }
}
