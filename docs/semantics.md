# Semantics (Phase 10 — G1–G10)

## CDC

```text
MySQL current tables
        ↓
Initial Snapshot (scan.startup.mode = initial)
        ↓
continuous binlog (ROW + FULL image + GTID)
        ↓
Flink SQL mysql-cdc → changelog (+I / -U / +U / -D)
        ↓
Paimon Primary Key ODS (merge-engine = deduplicate)
```

## Current state vs history

- **Current-state query** on `ods_*`: latest logical row per PK; DELETE → row absent.
- **G8 Time Travel**: dedicated `ods.ods_tt_demo` + `scan.snapshot-id` (not continuous CDC job TT).

## UPDATE / DELETE

- UPDATE keeps a single logical PK row (G3).
- DELETE retracts the PK row from current state (G4).

## Money

All money columns are `DECIMAL(12,2)` (or ADS `DECIMAL(18,2)` aggregates) end-to-end.
No `FLOAT` / `DOUBLE` for amounts. G9 reconcile uses `decimal.Decimal` only (tol 0.01).

## Schema Evolution (G5)

**ADD COLUMN** `orders.channel` via **explicit migration** (MySQL ALTER → Paimon ALTER → resubmit).
Not transparent Flink SQL CDC DDL. DROP/RENAME/type-change: not claimed.

## Failure Recovery (G6)

TM kill + checkpoint restore → practical catch-up. **Not** EO-2PC / Exactly-Once E2E.

## DWD / ADS (P5)

- DWD `dwd.dwd_orders`: order grain; `coupon_amount` NULL; `net_amount = amount`.
- ADS `ads.ads_order_daily`: batch `INSERT OVERWRITE` by `(dt, channel)`; NULL channel → `'unknown'`.
- Paid statuses: `paid` / `shipped` / `completed` (see `docs/dwd-ads.md`).

## Backfill (G7)

Date-scoped DWD/ADS repair; content fingerprint idempotent on re-run. Demo partition logic — not a general orchestrator.

## Reconcile (G9)

MySQL ↔ ODS current-state counts + `SUM(amount)` (total / dt / dt+channel). Demo check — not continuous monitoring.

## Compaction (G10)

Dedicated `ods.ods_compact_demo` (`write-only`); datagen writes; `CALL sys.compact(..., 'full')`.
Hard gate: query fingerprint identical. Soft: file count reduced. Not production sizing/latency SLA.

## Duplicates / recovery (honest)

Checkpointing enabled. Paimon PK upsert helps after at-least-once replay. Do not claim Exactly-Once E2E.
