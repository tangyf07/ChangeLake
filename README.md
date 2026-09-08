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
│ changelake │     │ UI :8081            │     │ file:///warehouse│
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

```bash
cp .env.example .env          # demo credentials only
make jars                     # download Paimon + shaded Hadoop jars
make up
make wait
```

等价：`bash scripts/bootstrap.sh`

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

浏览器打开：**http://localhost:8081**

### Paimon catalog stub

```bash
# jars 已下载且容器已启动后：
docker compose exec jobmanager ./bin/sql-client.sh -f /opt/flink/sql-changelake/paimon_catalog.sql
# 或手工粘贴 flink/sql/paimon_catalog.sql
```

Phase 1 仅创建 catalog + `ods`/`dwd`/`ads` database，不建业务表、不跑写入作业。

### 常用命令

```bash
make down      # 停容器，保留 named volumes
make reset     # 清空 volumes 后重新 up
make seed      # 重灌 seed=42
make status
make mysql-cli
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
