# AI Agent Instructions — Database Migration Project

This repository contains data migration logic between legacy datasets (`old_dataset`)
and new normalized datasets (`new_dataset`).

The primary task of the AI agent is to **analyze legacy data**, **generate correct PostgreSQL `INSERT` statements**,
**update the new dataset tables according to inserted data**, and **produce deterministic ID mapping files** for each migrated table.

---

## 📁 Project Structure

```
/old_dataset/     → Source data (read-only)
/new_dataset/     → Target dataset (modifiable)
/maps/            → Per-table ID mapping files (JSON, required)
/sql/             → Generated SQL scripts
/docs/            → Migration notes and mapping documentation
```

---

## 🔒 Critical Safety Rules (MANDATORY)

### 1) Old Dataset — READ ONLY
- ❌ Never modify, rename, or rewrite files in `old_dataset/`
- ❌ Never generate UPDATE or DELETE queries for old tables
- ✅ Only read data from this directory

### 2) New Dataset — Controlled Write Access
- ✅ INSERT operations are allowed
- ⚠ UPDATE operations only if explicitly requested (or if required to keep new dataset consistent with inserted data per the task)
- ❌ Never DROP tables
- ❌ Never TRUNCATE tables
- ❌ Never ALTER column definitions unless explicitly approved

### 3) Destructive / Risky Actions
- ❌ No schema redesign, no silent normalization, no data deletion
- ✅ If instructions conflict or are unclear: STOP and ask

---

## 📄 Dataset Format Rules

- All datasets are `.txt` files containing pipe-separated tables (`|`)
- First row is always a header row
- Example:

  ```
  |id|parent_id|name|client_id|
  |---|---------|----|---------|
  ```

### Value Handling
- If a **text** value contains non-English characters, replace them with English analogs (ASCII).
  - Example: `ã→a`, `ç→c`, `í→i`, `ñ→n`, `ö→o`, `ß→ss`, `æ→ae`.
  - Prefer deterministic normalization (Unicode NFKD + stripping diacritics) plus a small explicit substitution table for edge cases.
  - **Do NOT** modify secrets or machine values (passwords, hashes, tokens, signatures, base64) unless explicitly requested.
- If an object has `status = -1`, the object **must NOT** be inserted or kept in `new_dataset` (treat as excluded/disabled).
- Trim whitespace
- Convert empty values, `-`, or textual `NULL` to SQL `NULL`
- Preserve original casing unless instructed otherwise
- Escape strings for SQL using single quotes and standard PostgreSQL escaping

---

## 📤 SQL Output Rules

- Ensure normalized text values (non-English → English analogs) are reflected consistently in SQL and updated `new_dataset` files.

- PostgreSQL-compatible SQL only
- Always specify column names in `INSERT` statements
- One row per `INSERT` unless batching is explicitly requested
- Keep output deterministic and stable (same input → same SQL ordering)

Example:
```sql
INSERT INTO categories (id, parent_id, name, client_id)
VALUES (101, 10, 'Cameras', 3);
```

---

## 🗺️ ID Remapping + Mapping Files (REQUIRED)

### Why mapping is required
IDs of new objects may differ from IDs in the old dataset. For every migrated table/object type, the agent **must** create a **separate mapping file** stored in `/maps` as JSON.

### Mapping file requirements
- One mapping file per table/object type
- File location: `/maps/<table_name>.json` (or another clear table-name-based convention)
- Mapping must match **new objects to old objects** using **multiple fields**, not only names (names can be duplicated)
- Mapping must capture any referenced ID remaps (e.g., `client_id`, `role_id → role_ids`) to allow consistent downstream remapping

### Matching logic (high priority)
1) Prefer exact multi-field matches using configured match keys (e.g., email, fullname, status, settings, client_id, role relationships, etc.).
2) If the new object has **2+ completely exact old analogs**:
   - Choose the analog that has meaningful connections to other objects.
   - Example: if two old clients are identical, keep/relate to the one that has **users connected** to it (or the one referenced by other tables).
3) Never guess a match when keys are insufficient.
   - If ambiguous, leave it unmapped and record it in `unmapped_old` / `unmapped_new`.

### Mapping JSON schema (MUST FOLLOW)
Each mapping file must follow this structure:

```json
{
  "match_keys": [
    "email to email",
    "fullname to fullname",
    "role_id to role_ids",
    "status to status",
    "client_id to client_id",
    "settings to settings"
  ],
  "mapped": [
    {
      "old_id": 1,
      "new_id": 1,
      "email": "admin",
      "old_client_id": 0,
      "new_client_id": 0,
      "old_role_id": 1,
      "new_role_ids": [1]
    }
  ],
  "unmapped_old": [],
  "unmapped_new": []
}
```

### Mapping content rules
- `match_keys`: describe how matching was performed in plain, explicit terms (`<old_field> to <new_field>`).
- `mapped`: each entry must include `old_id`, `new_id`, plus enough identifying fields to audit the match.
- Include remapped references where relevant (e.g., `old_client_id`, `new_client_id`, `old_role_id`, `new_role_ids`).
- `unmapped_old`: list old records that could not be matched (include IDs and key fields if possible).
- `unmapped_new`: list new records that have no matching old analog.

---

## 🧱 Referential Integrity Rules

- Preserve all primary and foreign key relationships
- Maintain correct `parent_id → id` relationships
- If IDs are remapped, use mapping files to remap dependent tables consistently
- Never fabricate relationships; if a relationship can’t be mapped, record it and stop for clarification if it blocks the migration

---

## 🔄 Migration Workflow (Expected)

1) Read files from `old_dataset/` (no modifications)
2) Read files from `new_dataset/`
3) Determine which tables require inserts and/or edits in new dataset
4) Build mapping files in `/maps` for every affected table
5) Generate PostgreSQL `INSERT` statements using *new IDs* (or inserted IDs) as appropriate
6) Apply required updates to `new_dataset` tables to reflect inserted data and remapped references
7) Provide:
   - SQL scripts (text)
   - Updated `new_dataset` tables (text)
   - Mapping files (JSON)
   - Short explanation of transformations and any unresolved items

---

## 🧠 AI Behavior Rules

- If anything is ambiguous → STOP and ASK
- Never invent missing data
- Never assume matches with insufficient keys
- Prefer correctness and auditability over speed

---


---

## 📝 Agent Activity Log (AGENTS_LOG.md) — REQUIRED

To keep migration work auditable and consistent across sessions, the agent must maintain a lightweight activity log.

### Before starting any task
- ✅ **Read `AGENTS_LOG.md` immediately after reading `AGENTS.md`, before doing anything else.**
- If the repository uses `AGENT_LOG.MD` (singular) instead, treat it as the same log and read it as well.
- ✅ **Read `AGENTS_LOG.md`** (if present) to understand prior actions, assumptions, and any unresolved items.
- If the file does not exist, create it when you make the first log entry.

### While working
- ✅ Briefly log each meaningful action to `AGENTS_LOG.md` (append-only). Examples of actions:
  - “Parsed `old_dataset/clients.txt` and detected 2 duplicate client candidates by (email,name).”
  - “Generated `maps/users.json` using match keys: email, fullname, client_id, role_id→role_ids.”
  - “Produced SQL: `sql/clients_inserts.sql` (132 rows).”
  - “Updated `new_dataset/users.txt` by remapping client_id and role_ids.”
  - “Unmapped: 3 old records, 1 new record — see `maps/roles.json`.”

### Log format (recommended)
Use short, timestamped entries so they are easy to scan:

```markdown
## YYYY-MM-DD

- HH:MM UTC+00 — <action summary>
- HH:MM UTC+00 — <action summary>
```

(If you prefer local time, always state the timezone explicitly in each entry.)

### Do not log secrets
- ❌ Never log passwords, tokens, private keys, or sensitive personal data.


## 🧪 Validation Checklist (Before Output)

- [ ] No modifications to `old_dataset/`
- [ ] SQL syntax validated for PostgreSQL
- [ ] Referential integrity preserved (or issues documented)
- [ ] Mapping files created for every migrated table
- [ ] `unmapped_old` / `unmapped_new` lists populated where appropriate
- [ ] Output matches requested format

---

## 🚫 Forbidden Actions

- No destructive SQL (DROP/TRUNCATE/DELETE) unless explicitly instructed
- No silent “data fixing”
- No breaking changes to schema or API contracts without approval

---

## 🧭 Final Rule

If instructions conflict or are unclear:
**Stop and ask before continuing. Never assume.**
