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
- 17:05 UTC — Updated /helper_scripts/streams_migration.py to preserve only ids 1–4 from the existing dataset while rebuilding streams and to default missing addresses to empty strings for non-null inserts.
- 17:05 UTC — Regenerated new_dataset/streams_202512301039.txt, sql/streams_inserts.sql, and maps/streams.json with non-null addresses for migrated streams; preserved ids 1–4 as unmapped_new.
- 17:48 UTC — Added /helper_scripts/analytics_migration.py to clone stream_groups per plugin, remap client/stream/user references, and build analytics+analytics_groups artifacts.
- 17:48 UTC — Generated analytics_groups (1875 plugin-scoped rows) and migrated analytics (698 mapped, 626 unmapped; 9 existing preserved) updating datasets, SQL scripts, and mapping files.
- 18:12 UTC — Added /helper_scripts/event_manager_migration.py to normalize event manager rows, remap client_ids, and emit dataset/SQL/mapping outputs.
- 18:12 UTC — Migrated event_manager (327 mapped, 5 unmapped legacy due to missing clients; 2 existing preserved) regenerating new_dataset/event_manager_202512301039.txt, sql/event_manager_inserts.sql, and maps/event_manager.json.
- 18:32 UTC — Added /helper_scripts/alpr_lists_migration.py to normalize ALPR lists/items, remap clients/users/analytics references, and regenerate datasets/SQL/mapping artifacts.
- 18:32 UTC — Migrated active alpr_lists (14 rows) into new_dataset/alpr_lists_202512301039.txt, generated sql/alpr_lists_inserts.sql, and created maps/alpr_lists.json with unmapped legacy lists recorded for status=-1 or missing streams.
- 18:32 UTC — Migrated ALPR list items (466 rows tied to migrated lists) into new_dataset/alpr_list_items_202512301039.txt, generated sql/alpr_list_items_inserts.sql, and created maps/alpr_list_items.json.
- 18:43 UTC — Updated /helper_scripts/alpr_lists_migration.py to default unmapped/missing created_by values to admin id 1 for ALPR list items to satisfy NOT NULL constraint and regenerated new_dataset/sql/mapping outputs.
- 18:44 UTC — Added AGENTS.md guidance about defaulting ALPR list item created_by to admin id 1 when creator is unmapped.
## 2025-12-30

- 19:28 UTC — Added /helper_scripts/face_lists_migration.py to normalize legacy face lists, remap streams to face analytics, and emit dataset/SQL/mapping outputs.
- 19:28 UTC — Migrated active face_lists (status != -1) regenerating new_dataset/face_lists_202512301039.txt, sql/face_lists_inserts.sql, and maps/face_lists.json with unmapped streams noted.

- 20:32 UTC — Added helper_scripts/face_list_items_assets.py to build manifest and directories for face list assets, generating docs/face_list_items_images.json and face_lists_new skeleton.
- 20:32 UTC — Created img_rename.sh to rename and move face list images into face_lists_new using the generated manifest.
- 21:56 UTC — Updated /helper_scripts/face_list_items_assets.py to parse comma-separated legacy IDs so all items are retained in the manifest.
- 21:56 UTC — Regenerated docs/face_list_items_images.json and refreshed face_lists_new directories to include all 1,596 active face list items for image renaming.
- 22:30 UTC — Updated img_rename.sh to map face_lists files by filename against the manifest, warning on unmapped assets and avoiding duplicate moves.
- 22:57 UTC — Enhanced img_rename.sh logging (per-file actions, manifest gaps, summary) to diagnose why most assets are not moving.
- 23:23 UTC — Reworked img_rename.sh to load the manifest in a single jq pass, add error trapping, safe counters for set -e, and periodic progress logging.
- 23:24 UTC — Ran img_rename.sh; moved 531 images into face_lists_new, skipped 494 unmapped source files, and logged 1,110 manifest entries missing from source with per-file details in face_lists_new/img_rename.log.

## 2026-01-06

- 17:38 UTC — Updated img_rename.sh to allow MANIFEST/SOURCE_DIR/TARGET_ROOT environment overrides so renamed assets can be routed to alternate destinations.
- 17:38 UTC — Ran img_rename.sh with TARGET_ROOT=face_lists_new1; moved 532 images from face_lists, skipped 498 unmapped, and recorded 1,109 manifest-missing entries in face_lists_new1/img_rename.log.
## 2026-01-06
- 18:56 UTC — Investigated ALPR list item count gaps; discovered multiline comments in old_dataset/_alpr_list_items__202512301049.txt caused the parser to split 567 rows, dropping items (e.g., ÂNIMA PLACAS 110 entries -> 73).
- 18:56 UTC — Updated helper_scripts/alpr_lists_migration.py to parse multiline item rows safely, preserve existing item id mappings, and emit a backfill SQL for newly surfaced records.
- 18:56 UTC — Regenerated new_dataset/sql/mapping outputs for ALPR list items (721 active items, ANIMA PLACAS now 110) and created sql/alpr_list_items_backfill.sql with 255 missing records appended after prior ids.

## 2026-02-19

- 12:29 UTC — Rewrote old_migrator Java orchestration (MigratorsManager) to run the established helper_scripts pipeline in deterministic order and fail fast on script errors.
- 12:29 UTC — Replaced old_migrator/README.md with dataset-pipeline documentation and run instructions for the new Java script-runner migrator.
- 14:08 UTC — Reworked old_migrator Java logic to migrate directly from configurable MySQL/H2 dump into PostgreSQL with ordered table inserts, status filtering, text normalization, and ALPR created_by fallback.
- 14:08 UTC — Added integrated face list image rename step in Java migrator to move images into <target>/<list_id>/<item_id> naming convention after DB migration.
- 14:08 UTC — Updated old_migrator README and pom dependencies (H2/PostgreSQL drivers) for jar+config+dump runtime workflow.

## 2026-02-20
- 16:19 UTC — Extended old_migrator table migration order to include traffic_counters and traffic_stat so traffic analytics tables are copied into PostgreSQL during app migration.
- 16:19 UTC — Updated old_migrator/README.md migration-order documentation to include traffic_counters and traffic_stat.
- 16:21 UTC — Validation attempt: ran old_migrator build checks (`./mvnw test`, `mvn test`); wrapper is non-executable and Maven dependency download returned 403 from repo.maven.apache.org in this environment.
- 16:22 UTC — Retried wrapper via `sh ./mvnw test`; wrapper launched but could not download Maven distribution from repo.maven.apache.org in this environment.
- 16:28 UTC — Updated old_migrator analytics migration logic to preserve legacy analytics ids, generate deterministic UUID fallback when missing/invalid, and map stream_id to stream_uuid when possible.
- 16:28 UTC — Added post-insert identity sequence synchronization for migrated tables with id columns to avoid future id collisions after legacy-id inserts.
- 16:28 UTC — Updated old_migrator README rules to document legacy analytics id preservation and deterministic uuid fallback behavior.
- 16:29 UTC — Validation attempt: `sh ./mvnw test` still failed because Maven wrapper could not fetch Apache Maven zip from repo.maven.apache.org in this environment.
- 16:38 UTC — Added old_migrator/config.template.json as a ready-to-copy jar-side config template for MySQL/H2 dump to PostgreSQL migration.
- 16:38 UTC — Updated old_migrator/README.md runtime step to instruct copying config.template.json to config.json before running migrator.jar.
- 17:08 UTC — Extended old_migrator migration runtime to support configurable target DB type (`postgres` or `mysql`) including target-specific duplicate-ignore SQL and identity sync behavior.
- 17:08 UTC — Updated old_migrator config templates and README: added `target.type` in config.template.json and created old_migrator/config.mysql.template.json for MySQL target setup.
- 17:08 UTC — Validation attempt: `sh ./mvnw -q -DskipTests compile` failed because Maven wrapper could not fetch Apache Maven zip from repo.maven.apache.org in this environment.

- 17:29 UTC — Updated old_migrator DatabaseConfiguration to accept both legacy `db` and new `target` config.json sections, with null-safe validation and mysql/postgres DSLContext creation to prevent startup NPE when `db` is absent.
- 18:04 UTC — Hardened old_migrator DatabaseConfiguration config loading to support custom config path (`-Dconfig.path` / `MIGRATOR_CONFIG_PATH`), accept legacy `database` section, and throw explicit missing-config errors instead of null-pointer startup failures.
- 18:04 UTC — Updated old_migrator/README.md with config path override usage and troubleshooting steps for missing/invalid database config at startup.
- 18:17 UTC — Updated old_migrator/pom.xml to add spring-jdbc (for DriverManagerDataSource) and switch MySQL dependency to com.mysql:mysql-connector-j to fix compilation and remove relocation warning.
- 18:17 UTC — Validation attempt: `mvn clean package -DskipTests` in old_migrator failed in this environment because Maven Central access returned HTTP 403 while resolving spring-boot-starter-parent.
- 18:25 UTC — Investigated migrator startup OutOfMemoryError and traced it to `Files.readString` loading the full SQL dump in `MigratorsManager.openSourceConnection`.
- 18:25 UTC — Refactored `MigratorsManager` dump loading to stream and execute SQL statements incrementally (line-by-line preprocessing + statement batching) to avoid full-file memory allocation.
- 18:29 UTC — Disabled Spring `DataSourceAutoConfiguration` in `ReleasesMigratorApplication` so Boot no longer auto-starts an embedded H2 datasource when target config is MySQL/PostgreSQL.
- 18:33 UTC — Reworked dump parser in `MigratorsManager` to character-buffer streaming (no `readLine`), preventing giant single-line INSERTs from causing memory spikes on very large dumps (e.g., 32 GB).

- 18:50 UTC+00 — Updated old_migrator table migration order to include face_list_items and face_list_items_images so required face asset tables are not skipped.
- 18:50 UTC+00 — Enhanced old_migrator face image rename logic to resolve filenames from face_list_items.image or fallback join with face_list_items_images.path via list_item_id.
- 18:51 UTC+00 — Updated old_migrator/README.md table-order and face image mapping docs to reflect face_list_items_images-backed rename behavior.

- 18:59 UTC+00 — Hardened face image rename query: now resolves filename via COALESCE(face_list_items.image, face_list_items_images.path) with deterministic MIN(face_list_items_images.id) fallback per list item.
- 19:00 UTC+00 — Improved image filename extraction to support both Unix and Windows-style separators and enabled REPLACE_EXISTING during move to avoid duplicate-name move failures.
- 19:01 UTC+00 — Updated old_migrator/README.md face image mapping notes to document COALESCE fallback and deterministic first-image selection from face_list_items_images.

- 19:11 UTC+00 — Reworked face image rename mapping to keep deterministic winner per duplicated source filename using (list_id, item_id, image_order) precedence and avoid random overrides.
- 19:11 UTC+00 — Added deterministic destination suffixing (<item_id>_2, _3, ...) when multiple images resolve to the same list item/ext so extra images are not dropped.
- 19:12 UTC+00 — Updated old_migrator/README.md with deterministic multi-image destination naming behavior.
- 19:18 UTC+00 — Corrected face image source SQL to include all face_list_items_images.path rows when face_list_items.image is empty, preserving list_id <- list_item_id linkage and avoiding dropped extra item images.
- 19:18 UTC+00 — Updated old_migrator/README.md to document per-item precedence: use face_list_items.image if present, else use all linked face_list_items_images.path rows.
- 19:01 UTC+00 — Investigated user-reported Maven compilation error in old_migrator and traced it to `renameFaceImages` calling `resolveFaceImageSelectQuery` (throws SQLException) outside handled scope.
- 19:02 UTC+00 — Updated `old_migrator/src/main/java/com/incoresoft/releasesmigrator/MigratorsManager.java` to resolve face image select SQL inside the existing guarded block so checked SQL exceptions are wrapped consistently.
- 19:03 UTC+00 — Validation attempt: `mvn clean package -DskipTests` in old_migrator failed due to Maven Central HTTP 403 while resolving spring-boot-starter-parent, so full compile verification could not be completed in this environment.
