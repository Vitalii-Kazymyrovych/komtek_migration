# Java DB dump migrator

This application now performs end-to-end migration directly from an old DB dump (`mysql` or `h2`) into an existing PostgreSQL database.

## Runtime workflow

1. Put `migrator.jar` in a working folder.
2. Put `config.json` in the same folder.
3. Put the old DB dump in the same folder (or provide absolute path in config).
4. Put face images into the folder from config.
5. Run the app.

The app executes deterministic table migration order:

`clients -> roles -> users -> stream_groups -> streams -> analytics_groups -> analytics -> traffic_counters -> traffic_stat -> event_manager -> alpr_lists -> alpr_list_items -> face_lists`

Rules applied in migration:
- `status = -1` rows are skipped.
- `analytics.id` values are preserved from the legacy DB; missing/invalid `analytics.uuid` values are filled deterministically from legacy ids so inserts remain valid.
- Empty/`-`/`NULL` string values are converted to SQL `NULL`.
- Text values are normalized to ASCII (NFKD + deterministic substitutions).
- Sensitive machine fields (`password/hash/token/secret/signature/base64`) are not normalized.
- `alpr_list_items.created_by` defaults to `1` when missing.
- Inserts use `ON CONFLICT DO NOTHING` to avoid duplicate failures on reruns.

After DB rows are migrated, face list item images are moved/renamed to:
- `<images.target_dir>/<list_id>/<item_id>.<ext>`

## config.json example

```json
{
  "source": {
    "type": "mysql_dump",
    "dump_path": "./old_dump.sql"
  },
  "target": {
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
