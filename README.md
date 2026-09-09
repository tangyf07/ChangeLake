# ChangeLake

可复现的 CDC 增量湖仓演示：**MySQL → Flink CDC → Apache Paimon (MinIO)**。

> **当前仓库状态：Phase 4（Failure Recovery / G6）**  
> Flink SQL `mysql-cdc` → Paimon Primary Key ODS（current-state mirror）on **MinIO S3**。  
> Golden Path **G1–G6**（含 ADD COLUMN 显式迁移 + TaskManager kill / checkpoint restore）。  
> **尚未**实现 G7–G10、DWD/ADS、Backfill、全量 Reconcile、Compaction。  
> **不声称** EO-2PC / Exactly-Once E2E。

## Why

业务库里的可变关系数据，如何通过 CDC 进入现代湖仓，并在 UPDATE / DELETE / **DDL** / **TM 故障**后仍保持当前状态正确——Phase 4 在 G1–G5 基础上加入 Failure Recovery（checkpoint → kill TM → restore；不声称 EO-2PC）。

## Architecture Decision (storage)

**Stop:** `file:///warehouse`、warehouse 的 host bind mount、以及为本地 Paimon FS 准备的 chmod/root/VirtioFS 变通。

**Start:** MinIO（named volume `minio_data`）+ Paimon warehouse `s3://changelake/warehouse` + JAR `paimon-s3-1.4.2.jar`，目录选项按 [Paimon 1.4 Filesystems / S3](https://paimon.apache.org/docs/1.4/maintenance/filesystems/)（含 `s3.path.style.access=true`）。

**原因：** Docker Desktop 本地 FS / VirtioFS 上 Paimon `Mkdirs failed`；对象存储避开该类本地 mkdir 问题。Flink checkpoint 仍用 named volume（G6 依赖 `/checkpoints` 在 TM kill 后仍可读）。

## Architecture (Phase 4)

```text
MySQL 8.0.40 (ROW binlog + GTID)
        │  Initial Snapshot + binlog (+ explicit ADD COLUMN migration)
        ▼
Flink 1.18.1  +  flink-sql-connector-mysql-cdc 3.1.1
        │  checkpoint every 10s → file:///checkpoints (named volume)
        │  changelog (+I/-U/+U/-D); evolved job includes channel
        │  G6: TM kill → fixed-delay restart from checkpoint
        ▼
Paimon 1.4.2 PK tables  (paimon-s3-1.4.2.jar)
  warehouse: s3://changelake/warehouse  →  MinIO
  ods.ods_users / ods.ods_orders (+ channel after G5) / ods.ods_order_items
```

Named volumes: `changelake_mysql_data`, `changelake_minio_data`,
`changelake_flink_checkpoints`, `changelake_flink_savepoints`.

## Pinned versions

| Component | Version |
| --- | --- |
| MySQL | **8.0.40** (`mysql:8.0.40`) |
| Flink | **1.18.1** (`flink:1.18.1-scala_2.12-java17`) |
| Paimon | **1.4.2** → `paimon-flink-1.18-1.4.2.jar` |
| Paimon S3 | **1.4.2** → `paimon-s3-1.4.2.jar` (`org.apache.paimon:paimon-s3:1.4.2`) |
| Hadoop uber | `flink-shaded-hadoop-2-uber-2.8.3-10.0` |
| MinIO | `minio/minio:RELEASE.2025-07-23T15-54-02Z` (+ `minio/mc:RELEASE.2025-07-21T05-28-08Z`) |
| Flink CDC | **3.1.1** → `flink-sql-connector-mysql-cdc-3.1.1.jar` |
| MySQL JDBC | **8.0.33** → `mysql-connector-j-8.0.33.jar` |

- Paimon ↔ Flink: https://paimon.apache.org/docs/1.4/flink/quick-start/
- Paimon S3/MinIO: https://paimon.apache.org/docs/1.4/maintenance/filesystems/
- Flink CDC MySQL: https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/
- Schema evolution: [`docs/schema-evolution.md`](docs/schema-evolution.md)
- Failure recovery: [`docs/failure-recovery.md`](docs/failure-recovery.md)

## Quickstart

推荐运行顺序：**up → MinIO healthy/bucket → smoke_storage → demo_golden_path (G1→G6)**。

```bash
cp .env.example .env          # demo credentials only (incl. MinIO minioadmin/minioadmin)
# 若本机 8081 已被占用：
#   echo 'FLINK_UI_PORT=18081' >> .env   # 或编辑 .env
bash scripts/bootstrap.sh --jars-only
docker compose up -d
bash scripts/wait_services.sh
bash scripts/smoke_storage.sh          # Flink → Paimon → MinIO MUST PASS
bash scripts/demo_golden_path.sh       # G1→G6
```

有 GNU Make 时：

```bash
cp .env.example .env
make jars && make up && make wait
make smoke-storage
make demo
```

一键：`bash scripts/bootstrap.sh`（jars + up + wait）。

### Flink UI / MinIO console

- Flink UI 默认：**http://localhost:8081**（`.env` → `FLINK_UI_PORT`）
- MinIO API：**http://localhost:9000**；Console：**http://localhost:9001**（demo keys）

### Start CDC pipeline

```bash
bash scripts/start_pipeline.sh   # baseline schema (no channel)
# 或: make pipeline
```

停止：`bash scripts/stop_pipeline.sh` / `make stop-pipeline`。

Schema evolution（需 pipeline 已 RUNNING）：

```bash
bash scripts/schema_evolution.sh
# 或: make schema-evolution
```

Failure recovery / G6（需 pipeline 已 RUNNING；compose 已启用 10s checkpoint + fixed-delay restart）：

```bash
bash scripts/failure_recovery.sh
# 或: make failure-recovery
```

### Storage smoke

```bash
bash scripts/smoke_storage.sh
# 或: make smoke-storage
```

期望：`[smoke_storage] PASS Flink → Paimon → MinIO write/read`（断言 **SELECT 结果行**含 `minio-ok`）。

### Golden Path G1–G6

```bash
bash scripts/demo_golden_path.sh
# 或: make demo
```

期望输出（摘要）：

```text
[G1] PASS initial snapshot
[G2] PASS insert
[G3] PASS update
[G4] PASS delete
[G5] PASS schema evolution
[G6] PASS failure recovery
ALL PASS (G1–G6)
```

任一失败：`exit 2`（不会仅 WARNING 后继续）。

| Case | 验证点 |
| --- | --- |
| G1 | MySQL ↔ ODS 行数 20/50/85 + 固定 PK spot-check |
| G2 | INSERT `order_id=900001` 出现在 `ods_orders` |
| G3 | UPDATE → 单行 `amount=199.99` `status=paid` |
| G4 | DELETE → current-state 查询为空 |
| G5 | ADD `channel`；`1→app` / `900002→web` / `2→NULL`；pipeline RUNNING |
| G6 | ≥1 checkpoint → kill TM → restore → Paimon == MySQL（`order_id=3,900003`） |

Evidence：`docs/evidence/g1_*.txt` … `g6_*.txt`（由 demo/G6 脚本写入；未跑 Docker 时 g6 为 NOT RUN stub）。

### Schema evolution support matrix (honest)

| Capability | Status |
| --- | --- |
| MySQL `ADD COLUMN channel` while job RUNNING | Scripted |
| Paimon `ALTER TABLE … ADD channel` | Scripted |
| Flink SQL mysql-cdc transparent runtime DDL | **Not supported** |
| Explicit migration (ALTER + evolved resubmit) | **G5 path** |
| DROP / RENAME / type change | Not tested |

详见 [`docs/schema-evolution.md`](docs/schema-evolution.md)。

### MySQL seed

| table | count (`seed=42`) |
| --- | ---: |
| users | **20** |
| orders | **50** |
| order_items | **85** |

金额均为 `DECIMAL(12,2)`。重新灌数：`make seed` / `bash scripts/seed.sh`。

## Guarantees (Phase 4 only)

```text
MinIO warehouse for Paimon (scripted smoke)
Initial snapshot + continuous CDC (scripted G1)
Primary-key current-state upsert (ODS)
INSERT / UPDATE / DELETE propagation (G2–G4)
Schema evolution within documented support matrix (G5 explicit migration)
TaskManager kill + checkpoint restore → ODS == MySQL (scripted G6)
Automated Golden Path G1–G6
```

**Not claimed:** Exactly-Once E2E / **EO-2PC**、透明 SQL-CDC DDL、G7–G10、生产 HA/SLA、checkpoint-on-S3、Pipeline YAML auto schema sync。

## Credentials

`.env.example` 仅为 **demo-only**（含 MinIO `minioadmin`/`minioadmin`），禁止用于生产。

## Limitations

详见 [`docs/limitations.md`](docs/limitations.md)。语义：[`docs/semantics.md`](docs/semantics.md)。架构：[`docs/architecture.md`](docs/architecture.md)。Schema Evolution：[`docs/schema-evolution.md`](docs/schema-evolution.md)。Failure Recovery：[`docs/failure-recovery.md`](docs/failure-recovery.md)。

## Layout

```text
docker-compose.yml          # mysql + minio (+ init) + flink JM/TM (10s CP + restart)
Makefile / .env.example
mysql/001_schema.sql  002_seed.sql  003_cdc_grants.sql
mysql/mutations/{insert,update,delete,schema_evolution*,failure_recovery*}.sql
flink/sql/paimon_catalog.sql  cdc_source.sql  ods.sql
flink/sql/submit_ods_pipeline.sql  submit_ods_pipeline_evolved.sql
scripts/schema_evolution.sh  failure_recovery.sh  demo_golden_path.sh  …
docs/schema-evolution.md  failure-recovery.md  limitations.md  golden-path.md
docs/evidence/g1..g6_*.txt
```

## Next phases (not in this commit)

Phase 5+: DWD/ADS、Backfill、Time Travel、Reconcile、Compaction（G7–G10）。
