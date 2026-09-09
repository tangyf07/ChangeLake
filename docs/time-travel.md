# Time Travel / G8 (Phase 7)

Demonstrate **Paimon snapshot time travel** on a dedicated demo table so Golden Path
ODS counts (`ods_orders` 20/50/85 story) stay intact.

## Choice of table

| Option | Used? | Why |
| --- | --- | --- |
| Mutate a known `ods_orders` row | No | Would disturb G1–G7 / P5 counts and DWD/ADS |
| **`ods.ods_tt_demo`** | **Yes** | Isolated PK table; insert/update/delete story for `order_id=100` |

Honest: this is a **snapshot-id based demo** of Paimon batch time travel.
It is **not** EO-2PC, **not** continuous CDC time travel on the live ODS job, and **not** G10.

## Story (three snapshots)

```text
S1  INSERT  order_id=100  amount=100.00  status=created
S2  UPDATE  amount=200.00  status=paid
S3  DELETE  → row absent in current state
```

After each mutation the script records **`snapshot_id`** + **`commit_time`** from
`ods.\`ods_tt_demo$snapshots\``.

## Current State Query vs Historical Snapshot Query

### Current State Query

Reads the **latest** table snapshot (default batch scan). After S3 DELETE, the row is gone:

```sql
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ods.ods_tt_demo
WHERE order_id = 100;
-- → empty
```

This is the same “current-state mirror” idea as ODS (`merge-engine` / PK upsert): you see
**now**, not history.

### Historical Snapshot Query

Pins a **past** snapshot with the Flink SQL OPTIONS hint (Paimon 1.4.2 + Flink 1.18):

```sql
-- S1: amount ≈ 100
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ods.ods_tt_demo /*+ OPTIONS('scan.snapshot-id'='1') */
WHERE order_id = 100;

-- S2: amount ≈ 200
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ods.ods_tt_demo /*+ OPTIONS('scan.snapshot-id'='2') */
WHERE order_id = 100;

-- S3: row absent at the post-DELETE snapshot
SELECT order_id, CAST(amount AS STRING) AS amount, status
FROM ods.ods_tt_demo /*+ OPTIONS('scan.snapshot-id'='3') */
WHERE order_id = 100;
```

Snapshot ids in a real run are whatever Paimon assigns (often starting at `1` on a fresh
table); the G8 script prints the concrete ids it recorded.

### Optional Flink 1.18 syntax

Flink 1.18+ also supports:

```sql
SELECT * FROM ods.ods_tt_demo
FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-09 11:00:00';
```

G8’s **pass criteria** use **`scan.snapshot-id`** (deterministic). `FOR SYSTEM_TIME AS OF`
is attempted best-effort when `commit_time` is parseable and documented in evidence.

Docs: [Paimon 1.4 Flink SQL Query — Batch Time Travel](https://paimon.apache.org/docs/1.4/flink/sql-query/).

## How to run

```bash
# Stack up + jars + MinIO smoke (same as other phases):
make jars && make up && make wait && make smoke-storage

# Standalone G8 (only needs Flink + Paimon/MinIO; no CDC required):
bash scripts/time_travel.sh
# or:
bash scripts/verify_time_travel.sh
# or:
make time-travel

# Full golden path (G8 after G7):
bash scripts/demo_golden_path.sh
# or: make demo
```

Success line: **`[G8] PASS time travel`**.

Evidence: `docs/evidence/g8_time_travel.txt` (snapshot ids, commit times, PASS / NOT RUN stub).

## Retention

Demo table is created with:

```text
'snapshot.num-retained.min' = '10'
'snapshot.num-retained.max' = '50'
```

so short demos are unlikely to expire S1–S3 before assertions.

## What is NOT claimed

- **EO-2PC** / Exactly-Once end-to-end
- Time travel as a substitute for CDC correctness proofs (G1–G6)
- G10 compaction (Phase 9+); G9 reconcile is separate (`docs/reconcile.md`)
- Guaranteed long-term snapshot retention in production
- Continuous streaming time travel on the live `changelake-ods-cdc` job
