## 2025-12-30

- 12:32 UTC — Reviewed AGENTS.md instructions and began clients migration analysis.
- 12:33 UTC — Parsed old_dataset/_clients__202512301049.txt and normalized client text (ASCII, trimmed, unquoted comments).
- 12:33 UTC — Updated new_dataset/clients_202512301039.txt with migrated client records (ids 0–103) using normalized fields.
- 12:33 UTC — Generated sql/clients_inserts.sql with INSERT statements for all clients and maps/clients.json capturing 1:1 old→new id mapping.
- 12:48 UTC — Added batching rule (one INSERT per table) to AGENTS.md SQL section per user request.
- 12:48 UTC — Migrated roles from old_dataset/_roles__old.txt into new_dataset/roles_202512301208.txt, generated batched SQL (sql/roles_and_users_inserts.sql), and created maps/roles.json.
- 12:48 UTC — Migrated active users (status != -1) from old_dataset/_users__202512301159.txt into new_dataset/users_202512301208.txt with normalized names, updated batched SQL, and created maps/users.json.
- 14:44 UTC — Regenerated maps/users.json to align legacy user ids with manually added new_dataset/users_202512301440.txt entries and updated users_inserts.sql data.
