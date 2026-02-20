# Java DB dump migrator

This application now performs end-to-end migration directly from an old DB dump (`mysql` or `h2`) into an existing target database (`postgres` or `mysql`).

## Runtime workflow

1. Put `migrator.jar` in a working folder.
2. Copy `config.template.json` to `config.json` in the same folder and edit values.
3. Put the old DB dump in the same folder (or provide absolute path in config).
4. Put face images into the folder from config.
5. Run the app.

By default the app reads `config.json` from the current working directory.
If you need a different location, pass one of:
- JVM property: `-Dconfig.path=C:\\Scripts\\config.json`
- Environment variable: `MIGRATOR_CONFIG_PATH=C:\\Scripts\\config.json`

The app executes deterministic table migration order:

`clients -> roles -> users -> stream_groups -> streams -> analytics_groups -> analytics -> traffic_counters -> traffic_stat -> event_manager -> alpr_lists -> alpr_list_items -> face_lists -> face_list_items -> face_list_items_images`

Rules applied in migration:
- `status = -1` rows are skipped.
- `analytics.id` values are preserved from the legacy DB; missing/invalid `analytics.uuid` values are filled deterministically from legacy ids so inserts remain valid.
- Empty/`-`/`NULL` string values are converted to SQL `NULL`.
- Text values are normalized to ASCII (NFKD + deterministic substitutions).
- Sensitive machine fields (`password/hash/token/secret/signature/base64`) are not normalized.
- `alpr_list_items.created_by` defaults to `1` when missing.
- Inserts use target-specific duplicate-ignore SQL (`ON CONFLICT DO NOTHING` for PostgreSQL, `ON DUPLICATE KEY UPDATE id = id` for MySQL).

After DB rows are migrated, face list item images are moved/renamed to:
- `<images.target_dir>/<list_id>/<item_id>.<ext>`

Image source filename mapping is resolved from:
- `face_list_items.image` when present for that item;
- if `face_list_items.image` is empty/missing, all `face_list_items_images.path` rows linked by `face_list_items_images.list_item_id -> face_list_items.id` are used.

If multiple source files map to the same `<list_id>/<item_id>.<ext>` destination, additional files are saved deterministically as `<item_id>_2.<ext>`, `<item_id>_3.<ext>`, etc.

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

Supported `target.type` values:
- `postgres`
- `mysql`

Additional MySQL target template: `config.mysql.template.json`.

## Troubleshooting

- `Cannot invoke ... dbConfig is null` or `Database config is missing`
  - Ensure `config.json` exists in the working directory where you run `java -jar ...`.
  - Or explicitly set `-Dconfig.path=...` / `MIGRATOR_CONFIG_PATH`.
  - Ensure config has one of `db`, `target`, or `database` sections with `type`, `jdbc_url`, `user`, `password`.
