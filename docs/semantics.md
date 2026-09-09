# Semantics (Phase 4 scope: G1–G6)

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
- Historical snapshot / time travel is **out of scope** for Phase 2 (see G8 later).

## UPDATE

Paimon PK table keeps a single logical row for `order_id`. After UPDATE, G3 asserts `COUNT(*)=1` with `amount=199.99` and `status=paid`.

## DELETE

Source `DELETE` is applied as a retract on the PK table. G4 asserts current-state `COUNT(*)=0` for `order_id=900001`.

## Money

All money columns are `DECIMAL(12,2)` end-to-end (MySQL, Flink, Paimon). No `FLOAT` / `DOUBLE` for amounts.

## Duplicates / recovery (honest)

Flink checkpointing is enabled (`execution.checkpointing.interval=10s`, dir `file:///checkpoints`).
Phase 4 **G6** runs a TaskManager kill + restore experiment and asserts Paimon current-state
matches MySQL after catch-up. That proves **practical recovery**, **not** EO-2PC / Exactly-Once E2E.
Paimon PK upsert is idempotent w.r.t. duplicate same-key values, which helps after at-least-once replay.


## Schema Evolution (G5)

ChangeLake Phase 3 supports **ADD COLUMN** on `orders` → `channel VARCHAR(32)` via an
**explicit migration**, not transparent Flink SQL CDC DDL sync.

```text
MySQL ALTER ADD COLUMN (pipeline may stay RUNNING)
        ↓
Paimon ALTER TABLE ods.ods_orders ADD channel STRING
        ↓
Resubmit Flink SQL with evolved mysql_orders + SELECT channel
        ↓
ODS shows channel; old rows NULL; new DML syncs values
```

### Support matrix (honest)

| DDL | Status |
| --- | --- |
| ADD COLUMN (nullable) | **Tested** via G5 explicit migration |
| DROP COLUMN | Not tested / not claimed |
| RENAME COLUMN | Not tested / not claimed |
| ALTER COLUMN TYPE | Not tested / not claimed |

Flink SQL `mysql-cdc` 3.1.1 table schemas are fixed at submit time. Pipeline YAML schema
evolution is out of MVP scope. Details: [`schema-evolution.md`](schema-evolution.md).


## Failure Recovery (G6)

```text
≥1 completed checkpoint (Flink REST)
        ↓
MySQL UPDATE/INSERT (order_id=3, 900003)
        ↓
docker kill TaskManager → compose up taskmanager
        ↓
Job RUNNING (fixed-delay restart from checkpoint)
        ↓
More UPDATEs → ODS == MySQL
```

Details: [`failure-recovery.md`](failure-recovery.md).
