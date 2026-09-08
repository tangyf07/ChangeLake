# ChangeLake

可复现的 CDC 增量湖仓演示：**MySQL → Flink CDC → Apache Paimon**。

> **当前仓库状态：Phase 1（基础环境）**  
> Compose + MySQL + Flink JM/TM + Paimon filesystem catalog stub + 确定性 seed。  
> **尚无** CDC 作业、Golden Path G1–G10、DWD/ADS、Backfill/Reconcile/Compaction。

## Why

业务库里的可变关系数据，如何通过 CDC 进入现代湖仓，并在 UPDATE / DELETE / DDL / 故障 / 历史修复后仍可证明正确——这是 ChangeLake 的目标。Phase 1 先把可重复的本地底座搭好。

## Architecture (Phase 1)

```text
┌────────────┐     ┌─────────────────────┐     ┌──────────────────┐
│ MySQL 8.0  │     │ Flink 1.18.1 JM/TM  │     │ Paimon warehouse │
│ changelake │     │ UI :FLINK_UI_PORT   │     │ file:///warehouse│
│ seed=42    │     │ (no CDC job yet)    │     │ catalog stub     │
└────────────┘     └─────────────────────┘     └──────────────────┘
```

Named volumes: `changelake_mysql_data`, `changelake_paimon_warehouse`,
`changelake_flink_checkpoints`, `changelake_flink_savepoints`.

## Pinned versions

| Component | Version |
| --- | --- |
| MySQL | **8.0.40** (`mysql:8.0.40`) |
| Flink | **1.18.1** (`flink:1.18.1-scala_2.12-java17`) |
| Paimon | **1.4.2** → JAR `paimon-flink-1.18-1.4.2.jar` |
| Hadoop uber | `flink-shaded-hadoop-2-uber-2.8.3-10.0` |

Paimon ↔ Flink 兼容性依据官方文档：  
https://paimon.apache.org/docs/1.4/flink/quick-start/

## Quickstart

推荐（**不依赖 `make`**，WSL / 精简环境可用）：

```bash
cp .env.example .env          # demo credentials only
bash scripts/bootstrap.sh --jars-only
docker compose up -d
bash scripts/wait_services.sh
```

若已安装 GNU Make，也可：

```bash
cp .env.example .env
make jars && make up && make wait
```

一键：`bash scripts/bootstrap.sh`（含 jars + up + wait）。

### 验证 MySQL seed

```bash
docker compose exec -T mysql mysql -uchangelake -pchangelake -N -e "
SELECT 'users', COUNT(*) FROM changelake.users
UNION ALL SELECT 'orders', COUNT(*) FROM changelake.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM changelake.order_items;
"
```

**Exact seed counts (`seed=42`):**

| table | count |
| --- | ---: |
| users | **20** |
| orders | **50** |
| order_items | **85** |

金额字段全部为 `DECIMAL(12,2)`。订单状态集合：
`created`, `paid`, `shipped`, `completed`, `cancelled`, `refunded`。

重新灌数：`make seed`（幂等 DELETE + INSERT）。

### Flink UI

默认：**http://localhost:8081**（`.env` 中 `FLINK_UI_PORT`）。

若本机 `8081` 已被占用（常见于其它 Flink / 服务），改端口后再起：

```bash
# .env
FLINK_UI_PORT=18081
docker compose up -d
# 然后打开 http://localhost:18081
```

`scripts/wait_services.sh` 会读 `FLINK_UI_PORT`。

### Paimon catalog stub

```bash
# jars 已下载且容器已启动后：
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/paimon_catalog.sql
# 或手工粘贴 flink/sql/paimon_catalog.sql
```

Phase 1 仅创建 catalog + `ods`/`dwd`/`ads` database，不建业务表、不跑写入作业。

### 常用命令

```bash
# 有 make 时：
make down / make reset / make seed / make status / make mysql-cli

# 无 make 时：
docker compose down
docker compose down -v --remove-orphans && docker compose up -d && bash scripts/wait_services.sh
bash scripts/seed.sh
docker compose ps
docker compose exec mysql mysql -uchangelake -pchangelake changelake
```

重新生成 seed SQL：

```bash
python3 scripts/gen_sample_data.py
```

## MySQL schema (Phase 1)

见 `mysql/001_schema.sql`。`orders` **不含** `channel` / `coupon`（后续 Schema Evolution 再加）。

## Credentials

`.env.example` 中的账号密码仅为 **demo-only**，禁止用于生产。

## Limitations

详见 [`docs/limitations.md`](docs/limitations.md)。

摘要：

- 本地 Docker Compose demo，非生产部署
- 无 K8s / 多活 HA / 企业级目录
- **不**声称 Exactly-Once End-to-End 或生产 SLA
- Phase 1 **无** CDC / Golden Path / Backfill / Reconcile
- 部分环境（如精简 WSL）可能无 `make`：请用上方 bash / `docker compose` 等价命令
- 宿主机 `8081` 冲突时设置 `FLINK_UI_PORT`（验收实例曾用 `18081`）

## Layout

```text
README.md
LICENSE
Makefile
docker-compose.yml
.env.example
requirements.txt / pyproject.toml
mysql/001_schema.sql
mysql/002_seed.sql
mysql/my.cnf
flink/config/flink-conf.yaml
flink/sql/paimon_catalog.sql
flink/lib/                 # jars via make jars (.gitignored)
scripts/bootstrap.sh
scripts/wait_services.sh
scripts/seed.sh
scripts/gen_sample_data.py
docs/limitations.md
paimon/warehouse/.gitkeep
```

## Next phases (not in this commit)

Phase 2+：Flink CDC → Paimon ODS，Golden Path G1–G4（snapshot / insert / update / delete）。
