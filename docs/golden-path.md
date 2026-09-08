# Golden Path — Phase 2 (G1–G4)

Script: `scripts/demo_golden_path.sh` (also `make demo`).

| Case | Action | Pass criteria |
| --- | --- | --- |
| G1 | Start CDC pipeline (initial snapshot) | MySQL counts == ODS counts (20/50/85) + PK spot-checks |
| G2 | `mysql/mutations/insert.sql` (`order_id=900001`) | Row appears in `ods_orders` with matching fields |
| G3 | `mysql/mutations/update.sql` | Single row: `amount=199.99`, `status=paid` |
| G4 | `mysql/mutations/delete.sql` | Current-state query empty for `900001` |

Hard failure → print `FAIL`, write partial evidence if any, `exit 2`. Never WARNING-and-continue.

Evidence files (filled by the demo script):

```text
docs/evidence/g1_initial_snapshot.txt
docs/evidence/g2_insert.txt
docs/evidence/g3_update.txt
docs/evidence/g4_delete.txt
```

G5–G10 are **not** implemented in Phase 2.
