# Architecture (Phase 2)

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
         ▼
┌──────────────────┐
│ Paimon 1.4.2     │  file:///warehouse
│ ods.ods_users    │
│ ods.ods_orders   │  current-state mirror
│ ods.ods_order_items │
└──────────────────┘
```

No Kafka. No MinIO. Filesystem catalog only.
