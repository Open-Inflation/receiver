-- Add covering indexes for receiver ingestion and bridge polling under high write pressure.
-- Safe for repeated execution.

CREATE INDEX IF NOT EXISTS ix_task_runs_status_orchestrator_assigned_at
    ON task_runs (status, orchestrator_id, assigned_at);

CREATE INDEX IF NOT EXISTS ix_run_artifacts_parser_ingested_id
    ON run_artifacts (parser_name, ingested_at, id);

CREATE INDEX IF NOT EXISTS ix_run_artifact_products_artifact_id_id
    ON run_artifact_products (artifact_id, id);
