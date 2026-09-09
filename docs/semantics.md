# Semantics (Phase 2 scope)

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

Flink checkpointing is enabled (`execution.checkpointing.interval=30s`). Phase 2 **does not** run a fault-injection experiment; do not read this as a proven EO-2PC guarantee. Paimon PK upsert is idempotent w.r.t. duplicate same-key values, which helps after at-least-once replay, but that is not an E2E exactly-once proof.
