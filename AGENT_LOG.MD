## 2025-12-30

- 11:31 UTC — Reviewed OLD_DDL.txt and NEW_DDL.txt focusing on clients, roles, and users schemas for column differences.
- 11:32 UTC — Parsed old_dataset and new_dataset client/role/user tables with accent-insensitive matching; resolved duplicate role and user candidates via permission overlap and active-status preference.
- 11:33 UTC — Generated mapping files maps/clients.json, maps/roles.json, and maps/users.json with mapped/unmapped records for migrated clients, roles, and users.
