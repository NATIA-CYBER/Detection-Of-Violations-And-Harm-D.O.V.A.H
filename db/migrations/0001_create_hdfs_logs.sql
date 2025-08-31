-- 0001_create_hdfs_logs.sql
-- Creates the base table for parsed HDFS logs + useful indexes.

CREATE TABLE IF NOT EXISTS hdfs_logs (
  id           BIGSERIAL PRIMARY KEY,
  ts           TIMESTAMPTZ NOT NULL,
  host         TEXT        NOT NULL,
  component    TEXT        NOT NULL,
  level        TEXT        NOT NULL,
  template_id  INTEGER,
  session_id   TEXT,
  labels       JSONB       DEFAULT '{}'::jsonb,
  schema_ver   TEXT,
  raw_message  TEXT
);

CREATE INDEX IF NOT EXISTS hdfs_logs_ts_idx        ON hdfs_logs (ts);
CREATE INDEX IF NOT EXISTS hdfs_logs_component_idx ON hdfs_logs (component);
CREATE INDEX IF NOT EXISTS hdfs_logs_tpl_idx       ON hdfs_logs (template_id);
