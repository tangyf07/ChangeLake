# Schema Evolution (Phase 3 / G5)

## Goal

While the CDC pipeline is running:

1. `ALTER TABLE orders ADD COLUMN channel VARCHAR(32);`
2. Downstream ODS gains a `channel` column (nullable)
3. Pre-evolution rows may have `channel = NULL`
4. New `INSERT` / `UPDATE` values for `channel` sync to `ods_orders`
5. Console: `[G5] PASS schema evolution`

## Honest support matrix (this repo’s stack)

Pinned versions: **MySQL 8.0.40 / Flink 1.18.1 / Flink CDC mysql-cdc 3.1.1 / Paimon 1.4.2 + paimon-s3**.

| Layer | Capability | Status in ChangeLake Phase 3 |
| --- | --- | --- |
| MySQL | `ADD COLUMN channel VARCHAR(32) NULL` | **Supported** (scripted) |
| Flink SQL `mysql-cdc` 3.1.1 | Transparent runtime DDL → expand Flink table schema without resubmit | **Not supported** |
| Flink CDC **Pipeline YAML** API | `schema.change.behavior` / `include.schema.changes` auto-DDL | **Not used** (MVP stays on Flink SQL STATEMENT SET) |
| Paimon 1.4.2 | `ALTER TABLE ... ADD channel STRING` | **Supported** (scripted) |
| End-to-end demo path | Explicit migration (documented below) | **Supported** (G5) |
| `DROP COLUMN` / `RENAME COLUMN` / `ALTER TYPE` | — | **Not tested / not claimed** |
| Second evolution `coupon_amount` | — | **Deferred** (optional later; not Phase 3 minimum) |

### Why not “transparent” on Flink SQL CDC?

The Flink SQL MySQL CDC connector documents a **fixed** `CREATE TABLE (...)` schema at job submit time
([Flink CDC 3.1 MySQL source](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/)).
Automatic schema evolution (ADD/DROP/RENAME/TYPE) is a **Pipeline YAML** feature
([Schema Evolution](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/core-concept/schema-evolution/) —
available on pipeline releases; not wired into this demo’s SQL STATEMENT SET path).

We do **not** pretend the SQL connector evolves itself. We implement and document an **explicit migration**.

## Explicit migration path (what G5 actually does)

```text
1. Pipeline RUNNING (baseline schema: no channel)
2. MySQL: ALTER TABLE orders ADD COLUMN channel VARCHAR(32)   # while job still RUNNING
3. Assert Flink job still RUNNING (pre-evolution Flink schema; extra MySQL fields ignored)
4. Paimon: ALTER TABLE ods.ods_orders ADD channel STRING      # lake schema evolves in place
5. Cancel baseline job
6. Submit flink/sql/submit_ods_pipeline_evolved.sql
   - mysql_orders includes channel
   - INSERT SELECT …, channel
   - does NOT DROP existing ODS tables (no full warehouse rebuild)
7. Wait evolved job RUNNING
8. UPDATE order_id=1 SET channel='app'
   INSERT order_id=900002 … channel='web'
9. Assert ODS: 1→app, 900002→web, order_id=2→NULL
```

Scripts:

- `mysql/mutations/schema_evolution.sql` — idempotent MySQL ADD COLUMN
- `mysql/mutations/schema_evolution_dml.sql` — post-evolution UPDATE/INSERT
- `scripts/schema_evolution.sh` — orchestrates steps 2–7
- `flink/sql/submit_ods_pipeline_evolved.sql` — evolved Flink SQL
- `scripts/demo_golden_path.sh` — G5 after G1–G4

## CDC / Paimon settings used

**Baseline job** (`submit_ods_pipeline.sql`):

- `'connector' = 'mysql-cdc'`, `'scan.startup.mode' = 'initial'`
- No schema-change connector options (none apply on SQL connector for transparent DDL)
- Paimon ODS PK tables with `'changelog-producer' = 'input'`

**Evolved job** (`submit_ods_pipeline_evolved.sql`):

- Same mysql-cdc options; **source DDL includes `channel STRING`**
- Sink `ods.ods_orders` includes `channel` (via prior `ALTER TABLE … ADD` or CREATE IF NOT EXISTS cold path)
- `'scan.startup.mode' = 'initial'` after resubmit so current MySQL state (including NULLs) upserts into ODS
- Paimon DDL: `ALTER TABLE ods.ods_orders ADD channel STRING;`
  ([Paimon SQL Alter](https://paimon.apache.org/docs/1.4/flink/sql-alter/))

There is **no** `'schema-changes.enabled'` / `'debezium.*'` switch that makes the Flink SQL
connector expand its table schema at runtime in this version — do not add fake options.

## How to run G5 locally

```bash
# Full golden path (recommended)
bash scripts/smoke_storage.sh
bash scripts/demo_golden_path.sh
# expect: [G5] PASS schema evolution

# Or after G1–G4 already green and pipeline RUNNING:
bash scripts/schema_evolution.sh
# then apply DML + assert, or:
bash scripts/schema_evolution.sh --with-dml
```

Makefile: `make schema-evolution` / `make demo`.

## Evidence

`docs/evidence/g5_schema_evolution.txt` is written by a successful local demo run.
Until then it may be a **NOT RUN** stub — scripts must still be ready.
