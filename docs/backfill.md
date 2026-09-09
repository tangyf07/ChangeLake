# Backfill / G7 (Phase 6)

Date-scoped, idempotent repair of **DWD** + **ADS** for one business day.

## Scenario

Historical lake metrics are wrong for a day (demo: deliberate DWD `amount`/`net_amount` +1
corruption for `BACKFILL_DT`, default **`2026-08-13`** — seed orders `12` + `39`).
**MySQL remains the source of truth.** Backfill snapshots MySQL for that `dt`, rewrites the
logical DWD partition, rebuilds ADS for that `dt` only, then reconciles.

Partition key (logical): `dt = DATE(order_ts)` / ADS `dt`.

This is **not** a full wipe-reload and **does not** require re-running full CDC.

## Flow

```text
MySQL orders WHERE DATE(order_ts)=dt
        │  snapshot (incl. channel if column exists)
        ▼
DELETE dwd.dwd_orders WHERE CAST(order_ts AS DATE)=dt
INSERT VALUES … (net_amount=amount; coupon_amount=NULL)
        │
        ▼
DELETE ads.ads_order_daily WHERE dt=dt
INSERT aggregates FROM dwd WHERE CAST(order_ts AS DATE)=dt
        │  paid set ('paid','shipped','completed'); NULL channel → 'unknown'
        ▼
Reconcile MySQL ↔ DWD/ADS for dt
Fingerprint (canonical sort + SHA256)
```

SQL helpers:

- `flink/sql/backfill_dwd_dt.sql.tpl` — DELETE + INSERT VALUES (filled by `scripts/backfill.sh`)
- `flink/sql/backfill_ads_dt.sql.tpl` — DELETE + INSERT SELECT for one `dt`

Paimon 1.4.2 PK tables are **not** physically partitioned by `dt`; “partition-scoped” means
**delete+insert for rows matching that `dt`** (other dates untouched). ADS must not
double-count on re-run because the `dt` slice is replaced before insert.

## How to run

```bash
# After stack is up, smoke PASS, and preferably P5 DWD/ADS already present:
bash scripts/backfill.sh 2026-08-13
# or:
make backfill DT=2026-08-13

# G7 demo (corrupt → backfill ×2 → fingerprint equality):
bash scripts/verify_backfill.sh
# or full golden path (G7 after P5):
bash scripts/demo_golden_path.sh
```

Success line: **`[G7] PASS backfill`**.

Evidence stub / run output: `docs/evidence/g7_backfill.txt`.

## Fingerprint definition

Content-only (see `python/fingerprint.py`):

1. **DWD lines** for `dt`:  
   `order_id|user_id|status|amount|channel|net_amount`  
   sorted lexicographically, amounts normalized to 2 decimal places, NULL → `NULL`.
2. **ADS lines** for `dt`:  
   `channel|order_cnt|paid_order_cnt|gmv|net_gmv|buyer_cnt`  
   sorted by channel, same numeric normalization.
3. `dwd_sha = SHA256(joined_lines)` · `ads_sha = SHA256(joined_lines)`  
4. **`fingerprint = SHA256("dwd={dwd_sha}\\nads={ads_sha}\\n")`**

Does **not** depend on filenames, MinIO object keys, snapshot ids, or physical file layout.

Idempotency proof in G7: `fingerprint(run1) == fingerprint(run2)`.

## What is NOT claimed

- **EO-2PC** / Exactly-Once end-to-end
- A general production backfill **orchestrator** (Airflow/etc.) — this is **demo** partition-scoped logic
- Re-running full CDC / ODS rebuild as part of backfill
- **G9–G10** (time travel, full reconcile suite, compaction) / Phase 7+
- Continuous streaming ADS; physical Hive-style partitions on `dt`

## Relation to Phase 5

Reuses `dwd.dwd_orders` / `ads.ads_order_daily`, the same paid status set, NULL→`unknown`
channel mapping, and batch overwrite **pattern** (here scoped to one `dt` via delete+insert
instead of full-table `INSERT OVERWRITE`).
