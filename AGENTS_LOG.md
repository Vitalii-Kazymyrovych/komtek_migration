# AGENTS LOG

1. Inspected repository layout, schema files, and existing Java project structure.
2. Replaced the Maven project dependencies/build setup for a standalone migration CLI with Jackson, H2, PostgreSQL JDBC, and tests.
3. Implemented migration domain models (`Config`, `MappingSpec`).
4. Implemented `MappingGenerator` to compare `oldDDL.sql` and `newDDL.sql` and write `mapping.json` with direct, lookup, constant, and transformed mappings.
5. Implemented `MySqlDumpParser` that streams SQL files and parses multi-row INSERT statements safely across split dump files.
6. Implemented `MigrationService` to stage parsed MySQL rows into in-memory H2 and load transformed rows into PostgreSQL target schema.
7. Replaced application entrypoint to auto-generate `mapping.json` and execute migration with base path set to current folder.
8. Added AGENTS instructions file and started this action log.
9. Installed PostgreSQL 16, created role/database/schema, and loaded `newDDL.sql`.
10. Ran build and migration multiple times; fixed parser/mapping/runtime issues until migration completed successfully.
11. Added handling for INSERT statements without explicit column lists and for dump chunks split across files.
12. Added value coercion and special mappings (UUID handling, event_manager id -> uuid, face_lists send_internal_notifications fallback).
13. Verified PostgreSQL row counts for migrated tables.
14. Added Maven Shade plugin for executable fat JAR output.
15. Added PostgreSQL-safe typing/UUID coercion and table cleanup before insert to support repeated runs.
16. Validated shaded jar execution (`java -jar ...`) and verified migrated row counts in PostgreSQL.
17. Removed generated JAR artifacts from `target/` before commit.
