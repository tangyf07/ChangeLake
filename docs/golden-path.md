# Golden Path — Phase 7 (G1–G6 + P5 DWD/ADS + G7 Backfill + G8 Time Travel)

Script: `scripts/demo_golden_path.sh` (also `make demo`).

**Prereq:** MinIO healthy + bucket, then `scripts/smoke_storage.sh` PASS (Flink → Paimon → MinIO).
Order: G1 → G2 → G3 → G4 → G5 → G6 → **P5 (DWD/ADS)** → **G7 (Backfill)** → **G8 (Time Travel)**.

Standalone Phase 5 check: `bash scripts/verify_dwd_ads.sh` / `make dwd-ads`.  
Standalone G7 check: `bash scripts/verify_backfill.sh` / `make backfill DT=2026-08-13`.  
Standalone G8 check: `bash scripts/time_travel.sh` / `make time-travel`.

| Case | Action | Pass criteria |
| --- | --- | --- |
| G1 | Start CDC pipeline (initial snapshot) | MySQL counts == ODS counts (20/50/85) + PK spot-checks |
| G2 | `mysql/mutations/insert.sql` (`order_id=900001`) | Row appears in `ods_orders` with matching fields |
| G3 | `mysql/mutations/update.sql` | Single row: `amount=199.99`, `status=paid` |
| G4 | `mysql/mutations/delete.sql` | Current-state query empty for `900001` |
| G5 | `schema_evolution.sh` + DML | ADD `channel`; pipeline RUNNING; `1→app`, `900002→web`, `2→NULL` |
| G6 | `failure_recovery.sh` | ≥1 checkpoint → TM kill → restore → Paimon == MySQL (`3`, `900003`) |
| P5 | `verify_dwd_ads.sh` | DWD `net_amount` + ADS daily metrics vs MySQL for a known `dt` (needs **10** TM slots; DWD checkpoint before ADS) |
| G7 | `verify_backfill.sh` | Corrupt DWD for `BACKFILL_DT` → backfill ×2 → `fingerprint(run1)==fingerprint(run2)` + reconcile; `[G7] PASS backfill` |
| G8 | `time_travel.sh` | Dedicated `ods.ods_tt_demo`: S1 amount=100 → S2 amount=200 → S3 DELETE; query each `scan.snapshot-id`; `[G8] PASS time travel` |

Hard failure → print `FAIL`, write partial evidence if any, `exit 2`. Never WARNING-and-continue.

Evidence files (filled by the demo / verify scripts):

```text
docs/evidence/g1_initial_snapshot.txt
docs/evidence/g2_insert.txt
docs/evidence/g3_update.txt
docs/evidence/g4_delete.txt
docs/evidence/g5_schema_evolution.txt
docs/evidence/g6_failure_recovery.txt
docs/evidence/dwd_ads.txt
docs/evidence/g7_backfill.txt
docs/evidence/g8_time_travel.txt
```

**G9–G10** are **not** implemented in Phase 7.

Schema evolution: [`schema-evolution.md`](schema-evolution.md).  
Failure recovery: [`failure-recovery.md`](failure-recovery.md).  
DWD/ADS: [`dwd-ads.md`](dwd-ads.md).  
Backfill: [`backfill.md`](backfill.md).  
Time travel: [`time-travel.md`](time-travel.md).
