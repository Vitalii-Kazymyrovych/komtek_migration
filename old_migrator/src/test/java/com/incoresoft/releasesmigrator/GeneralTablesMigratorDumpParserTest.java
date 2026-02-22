package com.incoresoft.releasesmigrator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GeneralTablesMigratorDumpParserTest {

    @TempDir
    Path tempDir;

    @Test
    void parseDumpSupportsInsertIgnoreStatements() throws IOException {
        Path dump = tempDir.resolve("dump.sql");
        Files.writeString(
                dump,
                "INSERT IGNORE INTO videoanalytics.clients (`id`,`name`) VALUES (1,'Acme');\n",
                StandardCharsets.UTF_8
        );

        var parsed = GeneralTablesMigrator.DumpParser.parseDump(dump);

        List<GeneralTablesMigrator.DumpParser.Row> clientsRows = parsed.rowsByTable().get("clients");
        assertNotNull(clientsRows);
        assertEquals(1, clientsRows.size());
        assertEquals(Map.of("id", 1L, "name", "Acme"), clientsRows.getFirst().values());
    }

    @Test
    void parseDumpSupportsUtf16leDumpFiles() throws IOException {
        Path dump = tempDir.resolve("dump-utf16.sql");
        String content = "INSERT INTO videoanalytics.roles (`id`,`name`) VALUES (7,'Admin');\n";
        Files.write(dump, prependUtf16LeBom(content.getBytes(StandardCharsets.UTF_16LE)));

        var parsed = GeneralTablesMigrator.DumpParser.parseDump(dump);

        List<GeneralTablesMigrator.DumpParser.Row> roleRows = parsed.rowsByTable().get("roles");
        assertNotNull(roleRows);
        assertEquals(1, roleRows.size());
        assertEquals(Map.of("id", 7L, "name", "Admin"), roleRows.getFirst().values());
    }

    @Test
    void parseDumpSupportsFlexibleInsertWhitespace() throws IOException {
        Path dump = tempDir.resolve("dump-whitespace.sql");
        Files.writeString(
                dump,
                "INSERT   INTO `videoanalytics`.`clients` (`id`,`name`) VALUES (2,'Beta');\n"
                        + "INSERT\nINTO videoanalytics.roles (`id`,`name`) VALUES (9,'Operator');\n",
                StandardCharsets.UTF_8
        );

        var parsed = GeneralTablesMigrator.DumpParser.parseDump(dump);

        List<GeneralTablesMigrator.DumpParser.Row> clientsRows = parsed.rowsByTable().get("clients");
        assertNotNull(clientsRows);
        assertEquals(1, clientsRows.size());
        assertEquals(Map.of("id", 2L, "name", "Beta"), clientsRows.getFirst().values());

        List<GeneralTablesMigrator.DumpParser.Row> rolesRows = parsed.rowsByTable().get("roles");
        assertNotNull(rolesRows);
        assertEquals(1, rolesRows.size());
        assertEquals(Map.of("id", 9L, "name", "Operator"), rolesRows.getFirst().values());
    }

    private static byte[] prependUtf16LeBom(byte[] payload) {
        byte[] out = new byte[payload.length + 2];
        out[0] = (byte) 0xFF;
        out[1] = (byte) 0xFE;
        System.arraycopy(payload, 0, out, 2, payload.length);
        return out;
    }
}
