# Limitations (Phase 1)

ChangeLake Phase 1 只交付本地基础环境，**不**包含完整 CDC 湖仓能力。

## 本阶段有什么

- Docker Compose：MySQL 8.0.40 + Flink 1.18.1 JobManager/TaskManager
- 确定性 seed（`seed=42`）：`users=20` / `orders=50` / `order_items=85`
- Paimon **filesystem catalog stub**（SQL 已就绪，JAR 由 `make jars` 下载）
- Named volumes：MySQL data、Paimon `/warehouse`、Flink checkpoints/savepoints
- Flink UI：`http://localhost:8081`

## 本阶段没有什么

- 无 Flink CDC pipeline（无 Initial Snapshot / binlog 作业）
- 无 Golden Path G1–G10
- 无 ODS / DWD / ADS 表落地与指标
- 无 Backfill / Reconcile / Compaction / Time Travel 实验
- 无 Kafka / Airflow / Kubernetes / MinIO / Prometheus / Grafana / Web UI
- 无生产级 HA、无 Exactly-Once End-to-End 声明

## 版本与兼容性说明

| Component | Version | Notes |
| --- | --- | --- |
| MySQL | 8.0.40 | ROW binlog + GTID 已开启，供后续 CDC 使用 |
| Flink | 1.18.1 (`flink:1.18.1-scala_2.12-java17`) | UI port 8081 |
| Paimon | 1.4.2 | JAR: `paimon-flink-1.18-1.4.2.jar` |
| Hadoop uber | `flink-shaded-hadoop-2-uber-2.8.3-10.0` | filesystem warehouse 所需 |

兼容性来源：[Apache Paimon 1.4 Flink Quick Start](https://paimon.apache.org/docs/1.4/flink/quick-start/)  
（Paimon 1.4.2 提供 `paimon-flink-1.18-*.jar`，支持 Flink 1.16–1.20 / 2.x。）

## 语义边界（勿夸大）

- Phase 1 **不**验证 checkpoint 恢复后的数据正确性。
- Phase 1 **不**声称 Exactly-Once、生产 HA、零数据丢失。
- Demo 凭据仅用于本地（见 `.env.example`）。
- Paimon 写入能力取决于本机是否已执行 `make jars`；本仓库 Phase 1 CI/环境未必实际跑通 Flink→Paimon 写入。

## Schema 边界

- `orders` **尚未**包含 `channel` / `coupon_amount`（留给后续 Schema Evolution 阶段）。
