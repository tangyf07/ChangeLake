# Reconcile / G9 (Phase 8)

**Source ↔ lake current-state reconcile** for the ChangeLake demo.

Honest: this is a **point-in-time demo check** after CDC catch-up, **not** continuous
monitoring, **not** EO-2PC, and **not** an Exactly-Once end-to-end proof.

## What is compared

Preferred path: **MySQL ↔ Paimon ODS** (logical / current-state).

| Metric | Source | Lake | Pass rule |
| --- | --- | --- | --- |
| `users.count` | `COUNT(*)` `users` | `COUNT(*)` `ods.ods_users` | exact |
| `orders.count` | `COUNT(*)` `orders` | `COUNT(*)` `ods.ods_orders` | exact |
| `order_items.count` | `COUNT(*)` `order_items` | `COUNT(*)` `ods.ods_order_items` | exact |
| `orders.amount.total` | `SUM(amount)` | `SUM(amount)` ODS | `abs(diff) ≤ 0.01` |
| `orders.amount.dt=YYYY-MM-DD` | `SUM(amount)` by `DATE(order_ts)` | same on ODS | `abs(diff) ≤ 0.01` |
| `orders.amount.dt=….channel=…` | `SUM(amount)` by dt + channel | same on ODS | `abs(diff) ≤ 0.01` |

Optional (cheap, if `dwd.dwd_orders` has rows; `RECONCILE_INCLUDE_DWD=auto|yes|no`):

| Metric | Meaning |
| --- | --- |
| `dwd.orders.count_vs_mysql` | DWD row count vs MySQL orders |
| `dwd.orders.amount.total_vs_mysql` | DWD `SUM(amount)` vs MySQL |
| `dwd.orders.net_amount.total_vs_mysql` | DWD `SUM(net_amount)` vs MySQL (`net_amount=amount` while coupon absent) |

Money uses **`DECIMAL` only** (`python decimal.Decimal` in `python/reconcile_report.py`).
No float money compare. Default amount tolerance: **`0.01`** (`AMOUNT_TOLERANCE`).

## Channel / NULL policy

After G5, `orders.channel` / `ods.ods_orders.channel` may be NULL on pre-evolution rows.

| Layer | NULL channel handling |
| --- | --- |
| **ODS amount-by-channel (G9)** | Keep **NULL as NULL** (grouping sentinel `__NULL__`; report label `NULL`) |
| **ADS (Phase 5)** | Map NULL → literal **`unknown`** (see `docs/dwd-ads.md`) — **not** remapped in ODS G9 metrics |

Do not treat ADS `unknown` as equal to ODS `NULL` in the same metric row.

## Report format

Human table (also CSV + JSON):

```text
metric  source  lake  diff  status
```

Artifacts:

```text
docs/evidence/source_reconcile_report.csv
docs/evidence/source_reconcile_report.json
reports/source_reconcile_report.csv          # mirror
reports/source_reconcile_report.json
docs/evidence/g9_reconcile.txt               # PASS / FAIL stub + notes
```

Any row with `status=FAIL` → script **`exit 2`**.
Success line: **`[G9] PASS reconcile`**.

## How to run

```bash
# Stack up (same as other phases):
make jars && make up && make wait && make smoke-storage

# Prefer after golden path through G5+ (channel present) or full demo through G8:
bash scripts/reconcile.sh
# or:
make reconcile

# Full golden path (G9 after G8):
bash scripts/demo_golden_path.sh
# or: make demo
```

Env knobs:

| Var | Default | Meaning |
| --- | --- | --- |
| `AMOUNT_TOLERANCE` | `0.01` | DECIMAL abs(diff) gate for amount metrics |
| `RECONCILE_INCLUDE_DWD` | `auto` | `auto` = include DWD metrics if table non-empty |

Standalone note: if MySQL already has `channel`, G9 **does not** call baseline
`start_pipeline.sh` (that SQL `DROP`s ODS). It submits the **evolved** job or runs
`schema_evolution.sh` as needed.

## What is NOT claimed

- Continuous / scheduled monitoring or alerting
- EO-2PC / Exactly-Once end-to-end
- Bit-identical float-free Flink intermediate plans (compare uses DECIMAL at the report boundary)
- G10 compaction
- ADS full-suite re-check (use `verify_dwd_ads.sh` / P5); G9 optional DWD only
- Production SLA / multi-region consistency windows
