# Limitations (Phase 2)

ChangeLake Phase 2 delivers **MySQL → Flink CDC → Paimon ODS** for Golden Path **G1–G4** only.

## What Phase 2 includes

- Docker Compose: MySQL 8.0.40 + Flink 1.18.1 JobManager/TaskManager
- Deterministic seed (`seed=42`): `users=20` / `orders=50` / `order_items=85`
- Flink SQL CDC (`mysql-cdc` **3.1.1**) → Apache Paimon **1.4.2** Primary Key ODS tables
- ODS current-state mirror: `ods_users`, `ods_orders`, `ods_order_items`
- Automated Golden Path **G1–G4** (`scripts/demo_golden_path.sh`)
- Mutation helpers for INSERT / UPDATE / DELETE (`order_id=900001`)
- Flink UI via `FLINK_UI_PORT` (default `8081`; conflict example `18081`)
- Works without GNU Make (`bash` + `docker compose`)

## What Phase 2 does **not** include

- **G5–G10** (schema evolution, failure recovery, backfill, time travel, reconcile suite, compaction)
- DWD / ADS business metrics
- Kafka / Airflow / Kubernetes / MinIO / Prometheus / Grafana / Web UI / LLM
- Production HA, multi-region, enterprise catalog/lineage
- **Exactly-Once End-to-End** claims beyond documented Flink checkpoint + Paimon PK merge semantics

## Pinned versions & CDC support matrix

| Component | Version | Notes |
| --- | --- | --- |
| MySQL | 8.0.40 | ROW binlog + GTID; CDC user needs `REPLICATION SLAVE/CLIENT` |
| Flink | 1.18.1 (`flink:1.18.1-scala_2.12-java17`) | UI port via `FLINK_UI_PORT` |
| Paimon | 1.4.2 | JAR: `paimon-flink-1.18-1.4.2.jar` |
| Hadoop uber | `flink-shaded-hadoop-2-uber-2.8.3-10.0` | filesystem warehouse |
| Flink CDC | **3.1.1** | JAR: `flink-sql-connector-mysql-cdc-3.1.1.jar` |
| MySQL JDBC | 8.0.33 | JAR: `mysql-connector-j-8.0.33.jar` (not bundled in CDC SQL connector) |

CDC docs: [Flink CDC 3.1 MySQL source](https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/)  
Paimon docs: [Paimon 1.4 Flink Quick Start](https://paimon.apache.org/docs/1.4/flink/quick-start/)

### Verified in this phase (by design / script)

| Capability | Status |
| --- | --- |
| Initial Snapshot → ODS counts | G1 (scripted) |
| INSERT propagation | G2 (scripted) |
| UPDATE → single current-state row | G3 (scripted) |
| DELETE → row absent in current-state query | G4 (scripted) |
| ADD COLUMN / schema evolution | **Not in Phase 2** |
| TM kill + recovery | **Not in Phase 2** |

Local Docker E2E must be run on a machine with Docker; CI does not claim full CDC E2E.

## Semantics boundaries (do not over-claim)

- ODS tables are **current-state mirrors** (Paimon PK + `deduplicate`), not append-only CDC logs.
- Recovery correctness after TaskManager failure is **not** proven in Phase 2.
- Demo credentials only (see `.env.example`).
- Dataset is synthetic and laptop-scale.
- `start_pipeline.sh` drops/recreates ODS tables on each start (demo-friendly; not a production migration tool).

## Runtime notes

- If `Bind for 0.0.0.0:8081 failed`: set `FLINK_UI_PORT=18081` (or free port) in `.env`, then `docker compose up -d`.
- After downloading new jars, restart JM/TM so `/jars` is copied into `/opt/flink/lib` (`start_pipeline.sh` does this when CDC jar is missing inside the container).
- No `make`? Use the bash equivalents in README Quickstart.

## Docker Desktop / WSL2 filesystem note

On Docker Desktop (WSL2), Apache Paimon `LocalFileIO` may fail with `Mkdirs failed to create .../bucket-*` when `/warehouse` is a named volume or host bind mount. Phase 2 compose therefore mounts `/warehouse`, `/checkpoints`, and `/savepoints` as **tmpfs** so Golden Path can run locally. Data does not survive `docker compose down`. Native Linux Docker hosts may switch back to named volumes if preferred.
