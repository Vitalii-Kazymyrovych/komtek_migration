# Agent Instructions

1. Keep `AGENTS_LOG.md` updated with every significant action taken.
2. Update `README.md` after each code change so implementation notes stay current.
3. Runtime validation cycle:
   - Run `mvn clean package`.
   - Put JAR in folder with `config.json` and dump files.
   - Run migration.
   - Verify PostgreSQL tables are populated.
   - If exception occurs or target tables are empty: drop DB, recreate DB/schema, and rerun.
   - Repeat until migration works.
4. Do not commit generated runtime JAR artifacts.
