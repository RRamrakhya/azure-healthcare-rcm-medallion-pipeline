-- Audit/watermark table. NOTE: no IDENTITY/auto-increment column: concurrent ForEach iterations
-- inserting into it would serialize (the original project's parallelism bottleneck).
-- Uniqueness comes from (run_id, source_db, table_name).
CREATE TABLE IF NOT EXISTS audit.load_log (
  run_id        STRING NOT NULL,      -- ADF @pipeline().RunId
  source_db     STRING NOT NULL,
  table_name    STRING NOT NULL,
  load_type     STRING,
  rows_copied   BIGINT,
  max_watermark STRING,
  status        STRING,               -- Succeeded | Failed
  load_ts       TIMESTAMP
);
-- Watermark lookup used by the next incremental run:
-- SELECT coalesce(max(max_watermark), '1900-01-01') AS last_wm FROM audit.load_log
-- WHERE source_db = @{item().source_db} AND table_name = @{item().table_name} AND status = 'Succeeded';
