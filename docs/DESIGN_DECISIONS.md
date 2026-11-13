# Design decisions

**ADR-1 Metadata-driven ingestion.** One parameterized pipeline + config beats N hand-built pipelines: onboarding a table is a CSV row; `is_active` pauses a source without a deployment. Trade-off: config errors become runtime errors; validate config in CI (add a schema check).

**ADR-2 Audit table without identity.** Auto-increment forces serialized inserts; parallel ForEach then runs sequentially. Use natural uniqueness `(run_id, source, table)`.

**ADR-3 Hash surrogate keys.** `sha2(hospital_id||natural_id)` is deterministic (safe reruns/backfills, no coordination), avoids ID clashes between hospitals. Cost: 64-char string joins are slower than BIGINT; for BI marts consider a BIGINT dimension key assigned once in Gold.

**ADR-4 SCD2 via hash-diff.** One comparison column instead of comparing every attribute; MERGE with a NULL-merge-key staging trick expires and inserts in a single atomic statement; reruns are no-ops. Limitation: in-order arrival.

**ADR-5 Quarantine, not delete.** Reason-coded rows are kept in Silver and excluded from Gold; DQ metrics table tracks quarantine rate over time. Rules are declared once and rendered for Spark and tests.

**ADR-6 PII minimisation.** Real systems must not copy SSNs downstream. Hash with a secret salt and keep only what analytics needs. Data is synthetic here; the pattern is what matters. (Not legal/compliance advice; real PHI needs HIPAA controls.)

**ADR-7 Bronze Parquet, Silver Delta.** Bronze is cheap raw storage; Silver adds ACID/MERGE/time travel where updates occur.
