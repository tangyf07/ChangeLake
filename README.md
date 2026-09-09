# ChangeLake

可复现的 CDC 增量湖仓演示：**MySQL → Flink CDC → Apache Paimon (MinIO)**。

> **当前仓库状态：Phase 10（工程化收尾 / §27）**  
> Flink SQL `mysql-cdc` → Paimon ODS → **DWD** `dwd_orders` → **ADS** `ads_order_daily`（MinIO S3）。  
> Golden Path **G1–G6 + P5 + G7 + G8 + G9 + G10**（含日期范围幂等 Backfill + Paimon snapshot time travel + source↔lake reconcile + **demo compaction**）。  
> **不声称** EO-2PC / Exactly-Once E2E / 连续监控式 Reconcile / 连续流式 ADS / 通用 Backfill 编排器 / 生产级快照保留 SLA / **生产级 Compaction 容量或延迟 SLA**。

## Why

业务库里的可变关系数据，如何通过 CDC 进入现代湖仓，并在 UPDATE / DELETE / **DDL** / **TM 故障**后仍保持当前状态正确，再冻结 DWD 语义并产出 ADS 日指标，对历史错误日做 **分区级 Backfill**，用 Paimon **Snapshot Time Travel** 回看 INSERT→UPDATE→DELETE，用 **G9 Reconcile** 核对 MySQL↔ODS，最后在专用表上做 **G10 Compaction**（小文件合并后查询指纹不变）——Phase 10 在 G1–G10 已合入基础上补齐 pytest / 轻 CI / Makefile / docs / evidence；停工条件见说明 §27。

## Architecture Decision (storage)

**Stop:** `file:///warehouse`、warehouse 的 host bind mount、以及为本地 Paimon FS 准备的 chmod/root/VirtioFS 变通。

**Start:** MinIO（named volume `minio_data`）+ Paimon warehouse `s3://changelake/warehouse` + JAR `paimon-s3-1.4.2.jar`，目录选项按 [Paimon 1.4 Filesystems / S3](https://paimon.apache.org/docs/1.4/maintenance/filesystems/)（含 `s3.path.style.access=true`）。

**原因：** Docker Desktop 本地 FS / VirtioFS 上 Paimon `Mkdirs failed`；对象存储避开该类本地 mkdir 问题。Flink checkpoint 仍用 named volume（G6 依赖 `/checkpoints` 在 TM kill 后仍可读）。

## Architecture (Phase 10)

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
  dwd.dwd_orders  (Phase 5; net_amount = amount while coupon absent)
  ads.ads_order_daily  (Phase 5; dt + channel; NULL channel → 'unknown')
  G7 backfill: MySQL(dt) → replace DWD/ADS for that dt (bypass CDC)
  G8 time travel: ods.ods_tt_demo snapshots (S1/S2/S3) via scan.snapshot-id
  G9 reconcile: MySQL ↔ ODS counts + SUM(amount) (total / dt / dt+channel)
  G10 compaction: ods.ods_compact_demo (write-only batches → CALL sys.compact full)
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
- DWD/ADS: [`docs/dwd-ads.md`](docs/dwd-ads.md)
- Backfill / G7: [`docs/backfill.md`](docs/backfill.md)
- Time Travel / G8: [`docs/time-travel.md`](docs/time-travel.md)
- Reconcile / G9: [`docs/reconcile.md`](docs/reconcile.md)
- Compaction / G10: [`docs/compaction.md`](docs/compaction.md)

## Quickstart

推荐运行顺序：**up → MinIO healthy/bucket → smoke_storage → demo_golden_path (G1→G6→P5→G7→G8→G9→G10)**。

```bash
cp .env.example .env          # demo credentials only (incl. MinIO minioadmin/minioadmin)
# 若本机 8081 已被占用：
#   echo 'FLINK_UI_PORT=18081' >> .env   # 或编辑 .env
bash scripts/bootstrap.sh --jars-only
docker compose up -d
bash scripts/wait_services.sh
bash scripts/smoke_storage.sh          # Flink → Paimon → MinIO MUST PASS
bash scripts/demo_golden_path.sh       # G1→G6→P5→G7→G8→G9→G10
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

### DWD + ADS (Phase 5)

需 ODS 已有 `channel`（先跑 G5 / `schema_evolution.sh`）。校验脚本会尽量自启 ODS 并触发演进。

Flink `taskmanager.numberOfTaskSlots=10`（`docker-compose.yml` `FLINK_PROPERTIES`）。改完后需 `docker compose up -d --force-recreate taskmanager jobmanager`。提交顺序：DWD → ≥1 checkpoint → ADS batch `FINISHED`。

```bash
bash scripts/verify_dwd_ads.sh
# 或: make dwd-ads
# 仅提交作业: bash scripts/start_dwd_ads.sh / make start-dwd-ads
```

期望：`[DWD/ADS] PASS`；证据 `docs/evidence/dwd_ads.txt`。

### Backfill / G7 (Phase 6)

日期范围幂等修复：MySQL 快照（`DATE(order_ts)=dt`）→ 重写该日 DWD → 重建该日 ADS → reconcile。
**不**需要重跑全量 CDC；**不**全量 wipe-reload。

```bash
bash scripts/backfill.sh 2026-08-13
# 或: make backfill DT=2026-08-13
# G7 演示（注入 DWD 金额错误 → backfill 两次 → fingerprint 相等）:
bash scripts/verify_backfill.sh
```

期望：`[G7] PASS backfill`；证据 `docs/evidence/g7_backfill.txt`。详见 [`docs/backfill.md`](docs/backfill.md)。

### Time Travel / G8 (Phase 7)

专用表 **`ods.ods_tt_demo`**（不改 `ods_orders` / 金线计数）：S1 INSERT amount=100 → S2 UPDATE amount=200 → S3 DELETE；
记录 `snapshot_id` + `commit_time`；用 `/*+ OPTIONS('scan.snapshot-id'='N') */` 回看历史。

```bash
bash scripts/time_travel.sh
# 或: make time-travel
# 别名: bash scripts/verify_time_travel.sh
```

期望：`[G8] PASS time travel`；证据 `docs/evidence/g8_time_travel.txt`。详见 [`docs/time-travel.md`](docs/time-travel.md)。

### Reconcile / G9 (Phase 8)

MySQL ↔ Paimon ODS **当前状态**核对：表级行数（users / orders / order_items）+ `SUM(amount)`（总计、按 `dt`、按 `dt+channel`）。
金额只用 **DECIMAL**（`python/reconcile_report.py`），容差 **0.01**。报告列：`metric  source  lake  diff  status`。
ODS 金额按 channel 时 **NULL 保持 NULL**（ADS 的 `unknown` 仅 Phase 5；见 [`docs/reconcile.md`](docs/reconcile.md)）。

```bash
bash scripts/reconcile.sh
# 或: make reconcile
```

期望：`[G9] PASS reconcile`；证据 `docs/evidence/g9_reconcile.txt` + `docs/evidence/source_reconcile_report.{csv,json}`（`reports/` 有镜像）。

### Compaction / G10 (Phase 9)

专用表 `ods.ods_compact_demo`：`write-only=true` 下**分块**多批小写入（默认 30×100=3k 行，每 session 1 条 INSERT via `sql-client -f`）→ 等待 `COUNT(*)` → 记录 Before 统计与指纹 → Flink 1.18 `CALL sys.compact(..., 'full')` → After 统计与指纹。
**硬门禁**：查询结果指纹（ordered row dump SHA256）前后一致。**软期望**：`$files` 文件数下降（嘈杂时只记证据，不硬失败）。延迟仅记录，**不**要求百分比下降。

```bash
bash scripts/compaction.sh
# 或: make compaction
# 或: bash scripts/verify_compaction.sh
```

期望：`[G10] PASS compaction`；证据 `docs/evidence/g10_compaction.txt`。详见 [`docs/compaction.md`](docs/compaction.md)。


### Storage smoke

```bash
bash scripts/smoke_storage.sh
# 或: make smoke-storage
```

期望：`[smoke_storage] PASS Flink → Paimon → MinIO write/read`（断言 **SELECT 结果行**含 `minio-ok`）。

### Golden Path G1–G6 + P5 + G7 + G8 + G9 + G10

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
[DWD/ADS] PASS
[G7] PASS backfill
[G8] PASS time travel
[G9] PASS reconcile
[G10] PASS compaction
ALL PASS (G1–G6 + P5 + G7 + G8 + G9 + G10)
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
| P5 | DWD `net_amount` + ADS 日指标 vs MySQL（默认 `CHECK_DT=2026-08-02`） |
| G7 | 腐蚀 DWD → `backfill` ×2 → fingerprint 相等 + reconcile（默认 `BACKFILL_DT=2026-08-13`） |
| G8 | `ods.ods_tt_demo`：S1 amount=100 → S2 amount=200 → S3 DELETE；按 snapshot-id 回看 |
| G9 | MySQL ↔ ODS 行数 + `SUM(amount)`（total / dt / dt+channel）；DECIMAL tol 0.01；报告 CSV/JSON |
| G10 | `ods.ods_compact_demo`：多批小写入 → `CALL sys.compact` full；指纹前后一致（硬）；文件数通常下降（软） |

Evidence：`docs/evidence/g1_*.txt` … `g6_*.txt`、`dwd_ads.txt`、`g7_backfill.txt`、`g8_time_travel.txt`、`g9_reconcile.txt`、`g10_compaction.txt`、`source_reconcile_report.*`（由 demo / verify 写入；未跑 Docker 时为 NOT RUN stub）。

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

## Guarantees (Phase 10)

```text
MinIO warehouse for Paimon (scripted smoke)
Initial snapshot + continuous CDC (scripted G1)
Primary-key current-state upsert (ODS)
INSERT / UPDATE / DELETE propagation (G2–G4)
Schema evolution within documented support matrix (G5 explicit migration)
TaskManager kill + checkpoint restore → ODS == MySQL (scripted G6)
DWD order semantic freeze + ADS daily metrics vs MySQL (scripted P5)
Date-scoped backfill + idempotent fingerprint (scripted G7)
Paimon snapshot time travel on dedicated demo table (scripted G8)
MySQL ↔ ODS current-state reconcile, DECIMAL tol 0.01 (scripted G9)
Paimon demo compaction on dedicated table; fingerprint gate (scripted G10)
Automated Golden Path G1–G6 + P5 + G7 + G8 + G9 + G10
```

**Not claimed:** Exactly-Once E2E / **EO-2PC**、连续监控式 Reconcile、连续流式 ADS、透明 SQL-CDC DDL、**生产级 Compaction 容量/延迟 SLA**、通用 Backfill 编排器、`coupon_amount` CDC、生产 HA/SLA / 快照保留 SLA、checkpoint-on-S3、Pipeline YAML auto schema sync、连续 CDC 作业上的 time travel / compaction。

## CI vs local E2E

GitHub Actions (`ci.yml`) runs:

```text
python compileall
bash -n scripts/*.sh
pytest
```

**Full Golden Path is local Docker E2E** (`make demo` → G1–G10, prints `ALL PASS` + `DEMO_EXIT=0`).
CI does **not** pretend to cover Flink CDC + Paimon end-to-end.

```bash
make lint
make test   # or: make ci
make demo   # local Docker; requires jars + compose stack
```

## Credentials

`.env.example` 仅为 **demo-only**（含 MinIO `minioadmin`/`minioadmin`），禁止用于生产。

## Limitations

详见 [`docs/limitations.md`](docs/limitations.md)。语义：[`docs/semantics.md`](docs/semantics.md)。架构：[`docs/architecture.md`](docs/architecture.md)。Schema Evolution：[`docs/schema-evolution.md`](docs/schema-evolution.md)。Failure Recovery：[`docs/failure-recovery.md`](docs/failure-recovery.md)。DWD/ADS：[`docs/dwd-ads.md`](docs/dwd-ads.md)。Backfill：[`docs/backfill.md`](docs/backfill.md)。Time Travel：[`docs/time-travel.md`](docs/time-travel.md)。Reconcile：[`docs/reconcile.md`](docs/reconcile.md)。Compaction：[`docs/compaction.md`](docs/compaction.md)。

## Layout

```text
docker-compose.yml          # mysql + minio (+ init) + flink JM/TM (10s CP + restart)
Makefile / .env.example
mysql/001_schema.sql  002_seed.sql  003_cdc_grants.sql
mysql/mutations/{insert,update,delete,schema_evolution*,failure_recovery*}.sql
flink/sql/paimon_catalog.sql  cdc_source.sql  ods.sql  dwd.sql  ads.sql
flink/sql/submit_ods_pipeline.sql  submit_ods_pipeline_evolved.sql
flink/sql/submit_dwd_pipeline.sql  submit_ads_pipeline.sql
scripts/schema_evolution.sh  failure_recovery.sh  start_dwd_ads.sh  verify_dwd_ads.sh
scripts/demo_golden_path.sh  time_travel.sh  verify_time_travel.sh  reconcile.sh  compaction.sh  …
python/fingerprint.py  reconcile_report.py
tests/                    # pytest (CI)
.github/workflows/ci.yml
docs/schema-evolution.md  failure-recovery.md  dwd-ads.md  backfill.md  time-travel.md  reconcile.md  compaction.md
docs/evidence/g1..g10_*.txt  dwd_ads.txt  source_reconcile_report.*
reports/source_reconcile_report.*
```

## Phase 10 checklist (this branch)

- [x] pytest + static config tests
- [x] light GitHub Actions CI
- [ ] `make demo` ALL PASS + `DEMO_EXIT=0` (local evidence)
- [ ] §27 remaining boxes after demo evidence
