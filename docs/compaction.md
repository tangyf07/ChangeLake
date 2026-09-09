# Compaction / G10 (Phase 9)

**Demo compaction** for ChangeLake on a **dedicated** Paimon table
`ods.ods_compact_demo`.

Honest: this proves that **query results stay identical** across a full compact,
and usually that **file count drops**. It is **not** a production sizing SLA,
**not** a latency guarantee, and **not** EO-2PC.

## Why a dedicated table?

Golden-path ODS (`ods_users` / `ods_orders` / `ods_order_items`) carries G1–G9
counts and reconcile expectations. G10 therefore writes many small batches onto
**`ods.ods_compact_demo`** only so compaction does not disturb those proofs.

## Flow

1. `CREATE TABLE ods.ods_compact_demo … WITH ('write-only' = 'true', 'bucket' = '1')`  
   Writers skip compaction/snapshot expiration ([Paimon dedicated compaction](https://paimon.apache.org/docs/1.4/maintenance/dedicated-compaction/)).
2. Insert **many small batches** in **chunked** sql-client sessions (default **30 × 100 = 3 000** rows;
   `COMPACT_INSERTS_PER_SESSION` INSERTs per `-f` run, default **1**) → many L0 files / snapshots.
   Do **not** submit one giant multi-INSERT blob (that truncated mid-VALUES on the demo stack).
   After writes, poll `COUNT(*)` until it reaches the expected row count.
3. **Before**: snapshot count, file count, total `file_size_in_bytes`, query latency, content fingerprint (SHA256 of ordered `id/amount/status`).
4. Run compaction via Flink SQL procedure (batch, **full** strategy):

   ```sql
   -- Flink 1.18: positional args only ('' = placeholder)
   CALL sys.compact(
     'ods.ods_compact_demo',  -- table
     '',                      -- partitions
     '',                      -- order_strategy
     '',                      -- order_by
     'sink.parallelism=1',    -- options
     '',                      -- where
     '',                      -- partition_idle_time
     'full'                   -- compact_strategy (batch: merge all files)
   );
   ```

   Docs: [Paimon 1.4 Flink Procedures](https://paimon.apache.org/docs/1.4/flink/procedures/).
5. **After**: file count, size, latency, fingerprint again.
6. **Hard gate**: `fingerprint_before == fingerprint_after`.
7. **Soft expect**: `file_count_after < file_count_before` (documented if unchanged/noisy; **not** a hard fail). Latency % drop is **not** required.

## Stats source

| Metric | How |
| --- | --- |
| Snapshot count | `SELECT COUNT(*) FROM ods.\`ods_compact_demo$snapshots\`` |
| File count / size | `SELECT COUNT(*), SUM(file_size_in_bytes) FROM ods.\`ods_compact_demo$files\`` |
| Query latency | Wall time of ordered dump SQL (ms; noisy on tiny local data) |
| Fingerprint | SHA256 of sorted `id\tamount\tstatus` lines from current-state SELECT |

## Write path (G10)

Each chunk builds a small SQL file; `paimon_sql` **`docker compose cp`s** it into
`jobmanager` and runs **`sql-client.sh -f`**. Chunks wait for INSERT jobs to leave
RUNNING (FINISHED) before the next chunk. Scale defaults to **30×100** so the demo
stays reliable while still producing multiple commits / `file_count > 1` before compact.

## How to run

```bash
# Stack up + jars + MinIO smoke (same as other phases):
make jars && make up && make wait && make smoke-storage

# Standalone G10 (needs Flink + Paimon/MinIO; no CDC required):
bash scripts/compaction.sh
# or:
bash scripts/verify_compaction.sh
# or:
make compaction

# Full golden path (G10 after G9):
bash scripts/demo_golden_path.sh
# or: make demo
```

Env knobs:

| Variable | Default | Meaning |
| --- | ---: | --- |
| `COMPACT_BATCHES` | `30` | Number of INSERT commits |
| `COMPACT_ROWS_PER_BATCH` | `100` | Rows per commit (default **3k** total; override for larger) |
| `COMPACT_INSERTS_PER_SESSION` | `1` | INSERTs per sql-client `-f` session (1–5) |
| `COMPACT_ROW_WAIT_TIMEOUT` | `180` | Seconds to wait for `COUNT(*)` after writes |

Success line: **`[G10] PASS compaction`**.

Evidence: `docs/evidence/g10_compaction.txt` (before/after stats, fingerprints, PASS / NOT RUN stub).

## What is NOT claimed

- Production file-size / throughput / compaction-SLA sizing
- Guaranteed query-latency improvement (local demo data is tiny and noisy)
- EO-2PC / Exactly-Once end-to-end
- Continuous streaming compaction on the live `changelake-ods-cdc` job
- Compacting golden-path ODS tables as part of G1–G9
- Flink action JAR path as the primary demo (SQL `CALL sys.compact` is what G10 runs)
