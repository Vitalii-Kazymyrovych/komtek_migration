# SQL Dump Migration Tool (MySQL -> PostgreSQL)

This tool migrates selected data from MySQL dump files into PostgreSQL.

## Simple flow

1. The app compares `oldDDL.sql` and `newDDL.sql`.
2. It generates `mapping.json` with column mapping rules.
3. It reads `config.json` (DB credentials + dump **file names only**).
4. It streams all dump files and parses MySQL `INSERT` statements, even if one statement is split across files.
5. It stores parsed rows in temporary in-memory H2.
6. It transforms rows using `mapping.json` and inserts into PostgreSQL.

## Key Java classes

- `MigratorApplication` – starts the migration.
- `MappingGenerator` – creates `mapping.json` from DDL comparison.
- `MySqlDumpParser` – parses dump INSERT statements safely.
- `MigrationService` – stages data in H2 and writes to PostgreSQL.
- `Config` / `MappingSpec` – JSON models for `config.json` and `mapping.json`.

## Important behavior

- Base path is current directory (`.`), so keep jar + config + dump files together.
- `settings` is intentionally excluded from migration.
- Example advanced mapping: `analytics.stream_uuid` comes from `analytics.stream_id -> streams.id -> streams.uuid`.
- `face_lists.analytics_ids` is generated from legacy `face_lists.streams` by collecting `analytics.id` values whose `analytics.stream_id` belongs to the face-list stream set.
- Unmapped columns are listed in `mapping.json` (`unmappedTargetColumns`).

## Build and run

```bash
mvn clean package
java -jar target/migrator-1.0.0.jar
```

(Shade plugin creates a runnable fat JAR with dependencies.)

## Tables migrated

- analytics
- clients
- event_manager
- face_lists
- roles
- servers
- stats_traffic_minutely
- stream_groups
- streams
- traffic_stat
- traffic_counters (currently no source rows in old schema)
- users
