## 2025-12-30

- 12:32 UTC — Reviewed AGENTS.md instructions and began clients migration analysis.
- 12:33 UTC — Parsed old_dataset/_clients__202512301049.txt and normalized client text (ASCII, trimmed, unquoted comments).
- 12:33 UTC — Updated new_dataset/clients_202512301039.txt with migrated client records (ids 0–103) using normalized fields.
- 12:33 UTC — Generated sql/clients_inserts.sql with INSERT statements for all clients and maps/clients.json capturing 1:1 old→new id mapping.
- 12:48 UTC — Added batching rule (one INSERT per table) to AGENTS.md SQL section per user request.
- 12:48 UTC — Migrated roles from old_dataset/_roles__old.txt into new_dataset/roles_202512301208.txt, generated batched SQL (sql/roles_and_users_inserts.sql), and created maps/roles.json.
- 12:48 UTC — Migrated active users (status != -1) from old_dataset/_users__202512301159.txt into new_dataset/users_202512301208.txt with normalized names, updated batched SQL, and created maps/users.json.
- 14:44 UTC — Regenerated maps/users.json to align legacy user ids with manually added new_dataset/users_202512301440.txt entries and updated users_inserts.sql data.
- 14:52 UTC — Added /helper_scripts/stream_groups_migration.py to normalize stream group names, remap client_ids, and generate dataset/SQL/mapping outputs.
- 14:52 UTC — Migrated stream_groups (125 mapped rows) into new_dataset/stream_groups_202512301039.txt, generated sql/stream_groups_inserts.sql, and created maps/stream_groups.json; left old_id 83 unmapped due to missing client_id 41 in clients mapping.
- 15:19 UTC — Added /helper_scripts/streams_migration.py to normalize streams, remap client/stream_group/user references, and emit dataset/SQL/mapping artifacts for streams.
- 15:19 UTC — Migrated streams (414 mapped, 373 unmapped: 357 status=-1, 16 with missing client mappings) into new_dataset/streams_202512301039.txt, generated sql/streams_inserts.sql, and created maps/streams.json with creator_id remaps (unmapped creators stored as null).
- 15:28 UTC — Updated streams migration to preserve existing new_dataset rows and remap legacy streams with new sequential ids (starting at 5) to avoid collisions.
- 15:28 UTC — Regenerated new_dataset/streams_202512301039.txt (existing 4 rows retained, 414 legacy rows appended), sql/streams_inserts.sql (legacy inserts only, ids >=5), and maps/streams.json (documenting preserved new rows as unmapped_new and legacy unmapped due to status/client gaps).
