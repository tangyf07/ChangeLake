# Golden Path — Phase 3 (G1–G5)

Script: `scripts/demo_golden_path.sh` (also `make demo`).

**Prereq:** MinIO healthy + bucket, then `scripts/smoke_storage.sh` PASS (Flink → Paimon → MinIO).
Order: G1 → G2 → G3 → G4 → G5.

| Case | Action | Pass criteria |
| --- | --- | --- |
| G1 | Start CDC pipeline (initial snapshot) | MySQL counts == ODS counts (20/50/85) + PK spot-checks |
| G2 | `mysql/mutations/insert.sql` (`order_id=900001`) | Row appears in `ods_orders` with matching fields |
| G3 | `mysql/mutations/update.sql` | Single row: `amount=199.99`, `status=paid` |
| G4 | `mysql/mutations/delete.sql` | Current-state query empty for `900001` |
| G5 | `schema_evolution.sh` + DML | ADD `channel`; pipeline RUNNING; `1→app`, `900002→web`, `2→NULL` |

Hard failure → print `FAIL`, write partial evidence if any, `exit 2`. Never WARNING-and-continue.

Evidence files (filled by the demo script):

```text
docs/evidence/g1_initial_snapshot.txt
docs/evidence/g2_insert.txt
docs/evidence/g3_update.txt
docs/evidence/g4_delete.txt
docs/evidence/g5_schema_evolution.txt
```

G6–G10 are **not** implemented in Phase 3.

Schema evolution details: [`schema-evolution.md`](schema-evolution.md).
