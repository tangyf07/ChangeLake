# Architecture (Phase 3)

```text
┌──────────────────┐
│ MySQL 8.0.40     │  ROW binlog, GTID, server-id=1
│ changelake.*     │
└────────┬─────────┘
         │ mysql-cdc 3.1.1 (server-id ranges 5401–5412)
         ▼
┌──────────────────┐
│ Flink 1.18.1     │  JM + TM, checkpoint every 30s
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
│ ods.ods_orders   │  current-state mirror
│ ods.ods_order_items │
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
