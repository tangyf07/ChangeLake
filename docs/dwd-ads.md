# DWD + ADS (Phase 5)

Business semantic freeze and daily metrics on top of ODS.

## Scope

| Layer | Table | Role |
| --- | --- | --- |
| DWD | `dwd.dwd_orders` | Order-grain current state; `net_amount` rule |
| ADS | `ads.ads_order_daily` | Daily metrics by `(dt, channel)` |

**Not in Phase 5:** G7–G10, Backfill, Time Travel, Reconcile, Compaction, `coupon_amount` MySQL/ODS evolution.

## `dwd.dwd_orders` fields

| Column | Notes |
| --- | --- |
| `order_id` | PK |
| `user_id` | |
| `status` | from ODS |
| `amount` | `DECIMAL(12,2)` |
| `channel` | may be **NULL** for pre-G5 rows |
| `coupon_amount` | **NULL** in Phase 5 (column not in MySQL/ODS yet) |
| `net_amount` | `amount - COALESCE(coupon_amount, 0)` → equals **`amount`** while coupon is absent |
| `order_ts` | |
| `updated_at` | |

Table options: Primary Key + `merge-engine=deduplicate` + `changelog-producer=input`.

Requires `ods.ods_orders.channel` (post-G5 / evolved ODS). Pre-G5 NULL channels are preserved as NULL in DWD.

## `ads.ads_order_daily`

**Dimensions:** `dt` (= `CAST(order_ts AS DATE)`), `channel`.

**NULL channel mapping:** literal **`unknown`** (not `__NULL__`). Documented here and in evidence.

**Metrics only:**

| Metric | Formula |
| --- | --- |
| `order_cnt` | `COUNT(*)` all orders in group |
| `paid_order_cnt` | count where `status IN ('paid','shipped','completed')` |
| `gmv` | `SUM(amount)` for paid statuses only |
| `net_gmv` | `SUM(net_amount)` for paid statuses only |
| `buyer_cnt` | `COUNT(DISTINCT user_id)` for paid statuses only |

### Paid status set

```text
('paid', 'shipped', 'completed')
```

Aligned with seed statuses (`created` / `cancelled` / `refunded` are **not** paid for GMV).

## Pipelines

| Job | SQL | Mode |
| --- | --- | --- |
| `changelake-dwd-orders` | `flink/sql/submit_dwd_pipeline.sql` | **streaming** ODS → DWD (`scan.mode=latest-full`, checkpoint **10s**) |
| `changelake-ads-order-daily` | `flink/sql/submit_ads_pipeline.sql` | **batch** `INSERT OVERWRITE` DWD → ADS (`execution.runtime-mode=batch`) |

### Submit order (`scripts/start_dwd_ads.sh`)

1. Cancel any prior DWD / stuck ADS jobs (free TaskManager slots).
2. Submit **DWD** streaming job; wait until **RUNNING**.
3. Wait until DWD has **≥1 completed checkpoint** (Paimon sink commit), then poll `COUNT(*)` on `dwd.dwd_orders`.
4. Only then submit **ADS** batch `INSERT OVERWRITE`; wait until job state is **FINISHED** (not left `SCHEDULED`).

ADS uses a **bounded batch refresh** for Phase 5 correctness on Flink 1.18.1 + Paimon 1.4.2. Streaming continuous aggregation is **not** claimed.

### TaskManager slots

`taskmanager.numberOfTaskSlots: **10**` in `docker-compose.yml` `FLINK_PROPERTIES` (JM + TM) and `flink/config/flink-conf.yaml`.

Needed so **ODS** (≈6 tasks) + **DWD** (≈2) + **ADS** batch (1) + **sql-client collect** (1–2) can schedule together. With only 2 slots, ADS / verify `SELECT` stay `SCHEDULED` forever (slot starvation) while ODS+DWD hold the cluster.

After changing slots, recreate the TaskManager (and preferably JobManager) so the new value applies:

```bash
docker compose up -d --force-recreate taskmanager jobmanager
```

Catalog SQL follows existing patterns: session `CREATE CATALOG` (no `IF NOT EXISTS`), MinIO `s3://changelake/warehouse`, result-mode tableau for checks.

## How to run

```bash
# Prereq: stack up, smoke_storage PASS, ODS preferably evolved (channel present)
# Slots must be 10 (recreate JM/TM after pulling this change)
bash scripts/start_pipeline.sh
bash scripts/schema_evolution.sh   # if channel not yet on ODS
bash scripts/verify_dwd_ads.sh     # or: make dwd-ads
```

Optional: `CHECK_DT=2026-08-02 bash scripts/verify_dwd_ads.sh` (default `2026-08-02`).

Start jobs only: `bash scripts/start_dwd_ads.sh` / `make start-dwd-ads`.

Golden path: `demo_golden_path.sh` runs a **P5 / DWD+ADS** block after G6 (does **not** invent G7–G10).

## Evidence

`docs/evidence/dwd_ads.txt` — filled by `verify_dwd_ads.sh`. Repo ships a **NOT RUN** stub until a local Docker verify.

## What is NOT claimed

- EO-2PC / Exactly-Once E2E
- Continuous streaming ADS correctness without refresh
- `coupon_amount` CDC / schema evolution
- G7–G10 / Phase 6+
