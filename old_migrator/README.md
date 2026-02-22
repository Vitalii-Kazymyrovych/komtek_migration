# Java DB dump migrator

This application now performs end-to-end migration directly from an old DB dump (`mysql` or `h2`) into an existing target database (`postgres` or `mysql`).

## Runtime workflow

1. Put `migrator.jar` in a working folder.
2. Copy `config.template.json` to `config.json` in the same folder and edit values.
3. Put the old DB dump in the same folder (or provide absolute path/glob/directory in config).
4. Put face images into the folder from config.
5. Run the app.

By default the app reads `config.json` from the current working directory.
If you need a different location, pass one of:
- JVM property: `-Dconfig.path=C:\\Scripts\\config.json`
- Environment variable: `MIGRATOR_CONFIG_PATH=C:\\Scripts\\config.json`

`mapping.json` is resolved in this order:
1. Explicit path (`-Dmapping.path=...` or `MIGRATOR_MAPPING_PATH`)
2. Current working directory (`./mapping.json`)
3. Application directory (same folder as the JAR)

The app executes deterministic table migration order (general tables + mapped rename):

`analytics_groups -> api_tokens -> cleaning_settings -> clients -> event_manager -> face_lists -> plugin_configurations -> roles -> servers -> sounds_settings -> stats_traffic_minutely -> stream_groups -> streams -> system_settings -> traffic_counters -> traffic_stat -> users -> analytics -> smart_va_heatmap_plans (from heatmap_plans)`

Rules applied in migration:
- `status = -1` rows are skipped.
- `analytics.id` values are preserved from the legacy DB; missing/invalid `analytics.uuid` values are filled deterministically from legacy ids so inserts remain valid.
- Empty/`-`/`NULL` string values are converted to SQL `NULL`.
- Text values are normalized to ASCII (NFKD + deterministic substitutions).
- Sensitive machine fields (`password/hash/token/secret/signature/base64`) are not normalized.
- `alpr_list_items.created_by` defaults to `1` when missing.
- Inserts use target-specific duplicate-ignore SQL (`ON CONFLICT DO NOTHING` for PostgreSQL, `ON DUPLICATE KEY UPDATE id = id` for MySQL).

- Only `face_lists` is migrated directly in DB inserts; all other face tables are excluded from direct table migration.
- `face_list_items` and `face_list_items_images` are still parsed for filesystem preparation: files from `images.source_dir` are moved into per-list folders in `images.target_dir` and renamed to person names.

## config.json example

```json
{
  "source": {
    "type": "mysql_dump",
    "dump_path": "./old_dump.sql"
  },
  "target": {
    "type": "postgres",
    "jdbc_url": "jdbc:postgresql://127.0.0.1:5432/new_db",
    "user": "postgres",
    "password": "postgres"
  },
  "images": {
    "source_dir": "./face_lists",
    "target_dir": "./face_lists_new"
  }
}
```

Supported `source.type` values:
- `mysql_dump`
- `h2_dump`

`source.dump_path` accepts:
- a single file (for example `./old_dump.sql`)
- a glob pattern (for example `./old_dump_part_*.sql`)
- a directory (all `*.sql` files are loaded in lexical order)

Supported `target.type` values:
- `postgres`
- `mysql`

Additional MySQL target template: `config.mysql.template.json`.

## Troubleshooting

- `Cannot invoke ... dbConfig is null` or `Database config is missing`
  - Ensure `config.json` exists in the working directory where you run `java -jar ...`.
  - Or explicitly set `-Dconfig.path=...` / `MIGRATOR_CONFIG_PATH`.
  - Ensure config has one of `db`, `target`, or `database` sections with `type`, `jdbc_url`, `user`, `password`.
