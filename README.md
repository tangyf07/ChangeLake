# ChangeLake

可复现的 CDC 增量湖仓演示：**MySQL → Flink CDC → Apache Paimon**。

> **当前仓库状态：Phase 2（CDC 基础链路 / G1–G4）**  
> Flink SQL `mysql-cdc` → Paimon Primary Key ODS（current-state mirror）。  
> **尚未**实现 G5–G10、DWD/ADS、Backfill、全量 Reconcile、Compaction、Schema Evolution。

## Why

业务库里的可变关系数据，如何通过 CDC 进入现代湖仓，并在 UPDATE / DELETE 后仍保持当前状态正确——Phase 2 先把 Initial Snapshot + INSERT/UPDATE/DELETE 跑通并自动断言。

## Architecture (Phase 2)

```text
MySQL 8.0.40 (ROW binlog + GTID)
        │  Initial Snapshot + binlog
        ▼
Flink 1.18.1  +  flink-sql-connector-mysql-cdc 3.1.1
        │  changelog (+I/-U/+U/-D)
        ▼
Paimon 1.4.2 PK tables
  ods.ods_users / ods.ods_orders / ods.ods_order_items
```

Named volumes: `changelake_mysql_data`, `changelake_paimon_warehouse`,
`changelake_flink_checkpoints`, `changelake_flink_savepoints`.

## Pinned versions

| Component | Version |
| --- | --- |
| MySQL | **8.0.40** (`mysql:8.0.40`) |
| Flink | **1.18.1** (`flink:1.18.1-scala_2.12-java17`) |
| Paimon | **1.4.2** → `paimon-flink-1.18-1.4.2.jar` |
| Hadoop uber | `flink-shaded-hadoop-2-uber-2.8.3-10.0` |
| Flink CDC | **3.1.1** → `flink-sql-connector-mysql-cdc-3.1.1.jar` |
| MySQL JDBC | **8.0.33** → `mysql-connector-j-8.0.33.jar` |

- Paimon ↔ Flink: https://paimon.apache.org/docs/1.4/flink/quick-start/
- Flink CDC MySQL: https://nightlies.apache.org/flink/flink-cdc-docs-release-3.1/docs/connectors/flink-sources/mysql-cdc/

## Quickstart

推荐（**不依赖 `make`**）：

```bash
cp .env.example .env          # demo credentials only
# 若本机 8081 已被占用：
#   echo 'FLINK_UI_PORT=18081' >> .env   # 或编辑 .env
bash scripts/bootstrap.sh --jars-only
docker compose up -d
bash scripts/wait_services.sh
```

有 GNU Make 时：

```bash
cp .env.example .env
make jars && make up && make wait
```

一键：`bash scripts/bootstrap.sh`（jars + up + wait）。

### Flink UI

默认：**http://localhost:8081**（`.env` → `FLINK_UI_PORT`）。

```bash
# .env
FLINK_UI_PORT=18081
docker compose up -d
# → http://localhost:18081
```

`scripts/wait_services.sh` 与 Golden Path 都会读 `FLINK_UI_PORT`。

### Start CDC pipeline

```bash
bash scripts/start_pipeline.sh
# 或: make pipeline
```

这会：授予 CDC 复制权限 →（必要时）重启 JM/TM 加载 CDC JAR → 取消旧作业 → 提交
`flink/sql/submit_ods_pipeline.sql`（streaming STATEMENT SET）→ 等待 job `RUNNING`。

停止：`bash scripts/stop_pipeline.sh` / `make stop-pipeline`。

### Golden Path G1–G4

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
ALL PASS (G1–G4)
```

任一失败：`exit 2`（不会仅 WARNING 后继续）。

| Case | 验证点 |
| --- | --- |
| G1 | MySQL ↔ ODS 行数 20/50/85 + 固定 PK spot-check |
| G2 | INSERT `order_id=900001` 出现在 `ods_orders` |
| G3 | UPDATE → 单行 `amount=199.99` `status=paid` |
| G4 | DELETE → current-state 查询为空 |

Evidence：`docs/evidence/g1_*.txt` … `g4_*.txt`（由 demo 脚本写入）。

### MySQL seed

```bash
docker compose exec -T mysql mysql -uchangelake -pchangelake -N -e "
SELECT 'users', COUNT(*) FROM changelake.users
UNION ALL SELECT 'orders', COUNT(*) FROM changelake.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM changelake.order_items;
"
```

| table | count (`seed=42`) |
| --- | ---: |
| users | **20** |
| orders | **50** |
| order_items | **85** |

金额均为 `DECIMAL(12,2)`。重新灌数：`make seed` / `bash scripts/seed.sh`。

### 常用命令

```bash
make down / make reset / make seed / make pipeline / make demo / make status
# 无 make：
docker compose down
docker compose down -v --remove-orphans && docker compose up -d && bash scripts/wait_services.sh
bash scripts/seed.sh
bash scripts/start_pipeline.sh
bash scripts/demo_golden_path.sh
```

## Guarantees (Phase 2 only)

```text
Initial snapshot + continuous CDC (scripted G1)
Primary-key current-state upsert (ODS)
INSERT / UPDATE / DELETE propagation (G2–G4)
Automated Golden Path G1–G4
```

**Not claimed:** Exactly-Once E2E、G5–G10、生产 HA/SLA。

## Credentials

`.env.example` 仅为 **demo-only**，禁止用于生产。

## Limitations

详见 [`docs/limitations.md`](docs/limitations.md)。语义说明：[`docs/semantics.md`](docs/semantics.md)。

## Layout

```text
docker-compose.yml
Makefile / .env.example
mysql/001_schema.sql  002_seed.sql  003_cdc_grants.sql
mysql/mutations/{insert,update,delete}.sql
flink/config/flink-conf.yaml
flink/sql/paimon_catalog.sql  cdc_source.sql  ods.sql  submit_ods_pipeline.sql
flink/lib/                 # jars via bootstrap (.gitignored)
scripts/bootstrap.sh  wait_services.sh  seed.sh
scripts/start_pipeline.sh  stop_pipeline.sh
scripts/mutate_{insert,update,delete}.sh
scripts/demo_golden_path.sh  common.sh
docs/limitations.md  semantics.md  golden-path.md
docs/evidence/g1..g4_*.txt
```

## Next phases (not in this commit)

Phase 3+: Schema Evolution (G5)、Failure Recovery (G6)、DWD/ADS、Backfill、Time Travel、Reconcile、Compaction。
