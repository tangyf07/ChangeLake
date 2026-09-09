# Architecture (Phase 5)

```text
┌──────────────────┐
│ MySQL 8.0.40     │  ROW binlog, GTID, server-id=1
│ changelake.*     │
└────────┬─────────┘
         │ mysql-cdc 3.1.1 (server-id ranges 5401–5412)
         ▼
┌──────────────────┐
│ Flink 1.18.1     │  JM + TM, checkpoint every 10s (G6)
│ SQL STATEMENT SET│  job name: changelake-ods-cdc
└────────┬─────────┘
         │ PK upsert (changelog-producer=input)
         │ paimon-s3-1.4.2.jar
         ▼
┌──────────────────┐
│ MinIO            │  named volume minio_data
│ bucket changelake│
│ s3://changelake/warehouse
└────────┬─────────┘
         │
         ▼
┌──────────────────┐
│ Paimon 1.4.2     │  S3 warehouse (path-style)
│ ods.ods_users    │
│ ods.ods_orders   │  current-state mirror (+ channel after G5)
│ ods.ods_order_items │
│ dwd.dwd_orders   │  Phase 5 semantic freeze (net_amount)
│ ads.ads_order_daily │ Phase 5 daily metrics (batch refresh)
└──────────────────┘
```

## Architecture Decision — MinIO for Paimon warehouse

**Stop:** `file:///warehouse`, host bind mounts for the warehouse, and chmod/root/VirtioFS
workarounds aimed at local Paimon `LocalFileIO`.

**Start:** MinIO with named volume `minio_data`; Paimon catalog warehouse
`s3://changelake/warehouse` using official Paimon 1.4.2 S3 options
(`s3.endpoint`, `s3.access-key`, `s3.secret-key`, `s3.path.style.access=true`).

**Why:** On Docker Desktop (WSL2 / VirtioFS), Paimon local filesystem writers hit
`Mkdirs failed to create .../bucket-*`. Object storage avoids that class of local-FS
mkdir races for the lakehouse path. Flink checkpoints remain on named volumes for now
(not Phase 6 scope).

No Kafka / HDFS / Hive / Airflow.


## Schema evolution (Phase 3)

Baseline job uses `submit_ods_pipeline.sql` (no `channel`).
G5 uses an **explicit migration**: MySQL `ADD COLUMN` → Paimon `ALTER TABLE … ADD` →
resubmit `submit_ods_pipeline_evolved.sql` (does not DROP ODS).

Flink SQL `mysql-cdc` does not transparently expand table schemas at runtime; see
[`schema-evolution.md`](schema-evolution.md).


## Failure recovery (Phase 4)

G6 kills `changelake-taskmanager` after ≥1 completed checkpoint on named volume
`changelake_flink_checkpoints` (`file:///checkpoints`). JobManager stays up;
`restart-strategy.type: fixed-delay` brings the job back to RUNNING from the last
checkpoint; mysql-cdc resumes from stored offsets; Paimon PK ODS converges to MySQL.

See [`failure-recovery.md`](failure-recovery.md). **Not** an EO-2PC claim.


## DWD / ADS (Phase 5)

Streaming **ODS → DWD** (`changelake-dwd-orders`) freezes order-grain business fields
(`net_amount = amount` while `coupon_amount` is absent; NULL `channel` tolerated).

**ADS** `ads.ads_order_daily` is refreshed with a **batch** `INSERT OVERWRITE` from DWD
(dimensions `dt` + `channel` with NULL → literal `unknown`). See [`dwd-ads.md`](dwd-ads.md).

G7–G10 / Phase 6+ remain unimplemented. **Not** an EO-2PC claim.
