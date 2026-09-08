-- Flink CDC requires REPLICATION privileges (global).
-- Applied by scripts/start_pipeline.sh via root (initdb may already have run).
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT
  ON *.* TO 'changelake'@'%';
FLUSH PRIVILEGES;
