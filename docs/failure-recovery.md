# Failure Recovery (Phase 4 / G6)

## Goal

```text
checkpoint (≥1 completed)
        → MySQL UPDATE/INSERT
        → docker kill changelake-taskmanager
        → docker compose up -d taskmanager
        → job RUNNING again (restore from checkpoint + CDC catch-up)
        → more UPDATEs
        → Paimon ODS current-state == MySQL for affected rows
```

Console success: `[G6] PASS failure recovery`

## Proven (scripted)

| Item | Detail |
| --- | --- |
| Checkpoint interval | `execution.checkpointing.interval: 10s` (demo-friendly) |
| Checkpoint / savepoint dirs | `file:///checkpoints`, `file:///savepoints` on named volumes |
| State backend | `hashmap` |
| Retention | `RETAIN_ON_CANCELLATION` (externalized) |
| Restart | `restart-strategy.type: fixed-delay` (10 attempts, 5s delay) |
| Fault | `docker kill changelake-taskmanager` then recreate TM |
| Persistence | JM stays up; shared volume `changelake_flink_checkpoints` |
| Wait-for-checkpoint | Flink REST `GET /jobs/{jid}/checkpoints` → `counts.completed ≥ 1` |
| Final assert | `ods_orders` matches MySQL for `order_id=3` and `900003` |

Scripts: `scripts/failure_recovery.sh`, mutations under `mysql/mutations/failure_recovery_*.sql`.
Wired into Golden Path after G5 (`scripts/demo_golden_path.sh`).

## NOT claimed

- **Exactly-Once End-to-End** / **EO-2PC**
- Multi-JobManager HA / K8s failover
- Checkpoint storage on S3/MinIO (still named volume)
- Savepoint-based operator upgrade drills
- G7–G10 (backfill, time travel, reconcile, compaction)

Flink checkpoint + CDC offset restore + Paimon PK upsert give a **practical recovery demo**,
not a formal EO-2PC proof.

## How to run G6 locally

Prereq: stack up, jars installed, pipeline RUNNING (after `make demo` G1–G5, or
`make pipeline` / evolved job).

```bash
# Full golden path (recommended)
bash scripts/smoke_storage.sh
bash scripts/demo_golden_path.sh
# expect: [G6] PASS failure recovery
#         ALL PASS (G1–G6)

# Standalone (pipeline already RUNNING)
bash scripts/failure_recovery.sh
# or: make failure-recovery
```

After compose config changes (10s interval / restart-strategy), recreate JM/TM:

```bash
docker compose up -d --force-recreate jobmanager taskmanager
bash scripts/wait_services.sh
```

## Evidence

`docs/evidence/g6_failure_recovery.txt` is a **NOT RUN** stub until a local Docker run
overwrites it. Scripts are ready; authoring agents without Docker do not claim PASS.
