# Limitations (Phase 4)

ChangeLake Phase 4 delivers **MySQL → Flink CDC → Paimon ODS** for Golden Path **G1–G6**
(including schema evolution via **explicit migration** and **TM kill / checkpoint restore**).

## Architecture Decision (storage)

Paimon warehouse is **MinIO (S3-compatible)**, not `file:///warehouse`.

- **Why MinIO:** Docker Desktop local FS / VirtioFS caused Paimon `Mkdirs failed` on
  warehouse paths; host bind mounts + chmod workarounds were brittle.
- **What stayed local:** Flink checkpoints/savepoints use **named volumes** only
  (`file:///checkpoints`; G6 depends on this volume surviving TM kill — not moved to S3).
- Demo MinIO keys (`minioadmin` / `minioadmin`) are **demo-only**.

## What Phase 4 includes

- Docker Compose: MySQL 8.0.40 + Flink 1.18.1 JobManager/TaskManager + MinIO
- Deterministic seed (`seed=42`): `users=20` / `orders=50` / `order_items=85`
- Flink SQL CDC (`mysql-cdc` **3.1.1**) → Apache Paimon **1.4.2** Primary Key ODS tables on MinIO
- JAR `paimon-s3-1.4.2.jar` for S3 filesystem access
- ODS current-state mirror: `ods_users`, `ods_orders`, `ods_order_items`
- Storage smoke: `scripts/smoke_storage.sh` (Flink → Paimon → MinIO) before G1
- Automated Golden Path **G1–G6** (`scripts/demo_golden_path.sh`)
- Schema evolution **ADD COLUMN channel** via explicit migration
  (`scripts/schema_evolution.sh`, `docs/schema-evolution.md`)
- Failure recovery **G6** (checkpoint → kill TaskManager → restore)
  (`scripts/failure_recovery.sh`, `docs/failure-recovery.md`)
- Flink checkpoint interval **10s**, dir `file:///checkpoints`, fixed-delay restart
- Mutation helpers for INSERT / UPDATE / DELETE / schema evolution
- Flink UI via `FLINK_UI_PORT` (default `8081`; conflict example `18081`)
- Works without GNU Make (`bash` + `docker compose`)

## What Phase 4 does **not** include

- **G7–G10** (backfill, time travel, reconcile suite, compaction)
- DWD / ADS business metrics
- Kafka / HDFS / Hive / Airflow / Kubernetes / Prometheus / Grafana / Web UI / LLM
- Flink CDC **Pipeline YAML** auto schema evolution (we stay on Flink SQL STATEMENT SET)
- Transparent runtime DDL on Flink SQL `mysql-cdc` (not available — see support matrix)
- `DROP` / `RENAME` / type-change DDL (not tested)
- Optional second evolution `coupon_amount` (deferred)
- Production HA, multi-region, enterprise catalog/lineage
- Flink checkpoints on S3 (named volume only)
- **Exactly-Once End-to-End** / **EO-2PC** claims (G6 proves practical restore + catch-up, not EO-2PC)

## Pinned versions & CDC support matrix

| Component | Version | Notes |
| --- | --- | --- |
| MySQL | 8.0.40 | ROW binlog + GTID; CDC user needs `REPLICATION SLAVE/CLIENT` |
| Flink | 1.18.1 (`flink:1.18.1-scala_2.12-java17`) | UI port via `FLINK_UI_PORT` |
| Paimon | 1.4.2 | JAR: `paimon-flink-1.18-1.4.2.jar` |
| Paimon S3 | 1.4.2 | JAR: `paimon-s3-1.4.2.jar` (`org.apache.paimon:paimon-s3:1.4.2`) |
| Hadoop uber | `flink-shaded-hadoop-2-uber-2.8.3-10.0` | classpath support for FS plugins |
| MinIO | `minio/minio:RELEASE.2025-07-23T15-54-02Z` | warehouse + bucket `changelake` |
| Flink CDC | **3.1.1** | JAR: `flink-sql-connector-mysql-cdc-3.1.1.jar` |
| MySQL JDBC | 8.0.33 | JAR: `mysql-connector-j-8.0.33.jar` (not bundled in CDC SQL connector) |

CDC docs: [Flink CDC 3.1 MySQL source](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/)  
Paimon docs: [Paimon 1.4 Flink Quick Start](https://paimon.apache.org/docs/1.4/flink/quick-start/)  
Paimon S3/MinIO: [Filesystems](https://paimon.apache.org/docs/1.4/maintenance/filesystems/)  
Paimon ALTER: [SQL Alter](https://paimon.apache.org/docs/1.4/flink/sql-alter/)  
Schema evolution design: [`schema-evolution.md`](schema-evolution.md)

### Verified in this phase (by design / script)

| Capability | Status |
| --- | --- |
| MinIO healthy + bucket | compose health + `minio-init` / `scripts/minio_init.sh` |
| Flink → Paimon → MinIO write/read | `smoke_storage` (scripted; asserts SELECT row) |
| Initial Snapshot → ODS counts | G1 (scripted) |
| INSERT propagation | G2 (scripted) |
| UPDATE → single current-state row | G3 (scripted) |
| DELETE → row absent in current-state query | G4 (scripted) |
| ADD COLUMN channel (explicit migration) | G5 (scripted) |
| Transparent SQL-CDC DDL without resubmit | **Not supported / not claimed** |
| TM kill + checkpoint restore | G6 (scripted) |
| EO-2PC / Exactly-Once E2E | **Not claimed** |

Local Docker E2E must be run on a machine with Docker; CI / authoring agents do not claim full CDC E2E unless evidence files are filled by a local run.

## Semantics boundaries (do not over-claim)

- ODS tables are **current-state mirrors** (Paimon PK + `deduplicate`), not append-only CDC logs.
- Schema evolution for ADD COLUMN uses **job resubmit with evolved Flink SQL**, not Pipeline YAML auto-DDL.
- Recovery after TaskManager kill is **scripted in G6**; do **not** read that as EO-2PC.
- Demo credentials only (see `.env.example`), including MinIO `minioadmin`/`minioadmin`.
- Dataset is synthetic and laptop-scale.
- Baseline `start_pipeline.sh` drops/recreates ODS tables on each fresh start (demo-friendly).
  G5 evolved submit does **not** drop ODS (preserves lake + ADD COLUMN).

## Runtime notes

- If `Bind for 0.0.0.0:8081 failed`: set `FLINK_UI_PORT=18081` (or free port) in `.env`, then `docker compose up -d`.
- After downloading new jars (especially `paimon-s3`), restart JM/TM so `/jars` is copied into `/opt/flink/lib`.
- Recommended order: **MinIO healthy + bucket → `smoke_storage` PASS → Golden Path G1→G6**.
- No `make`? Use the bash equivalents in README Quickstart.
