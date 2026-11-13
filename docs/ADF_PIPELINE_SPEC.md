# ADF pipeline spec: `pl_metadata_driven_ingest` (design; export JSON from ADF Git integration after rebuilding)

1. **Lookup `lkp_config`** reads `config/load_config.csv` (first row as header, all rows).
2. **ForEach `fe_tables`**, items `@activity('lkp_config').output.value`, **isSequential = false**, batch count 10.
3. Inside: **If `chk_active`**, expression `@equals(item().is_active, 1)` (CSV values are strings, so cast: `@equals(int(item().is_active), 1)`).
4. True branch: **Lookup `lkp_watermark`** (query audit table for last successful watermark) → **Switch** on `item().load_type`:
   - `full`: Copy with `SELECT * FROM @{item().table_name}`
   - `incremental`: Copy with `SELECT * FROM @{item().table_name} WHERE @{item().watermark_column} > '@{activity('lkp_watermark').output.firstRow.last_wm}'`
5. **Before copy:** Get Metadata on target path → If exists → Copy old file to `/archive/<yyyy-MM-dd>/` (keeps history).
6. **After copy:** Stored Procedure / Script writes `audit.load_log` row: `run_id=@{pipeline().RunId}`, `rows_copied=@{activity('copy').output.rowsCopied}`, `max_watermark`, status.
7. **On failure:** log `Failed` row + alert (Azure Monitor action group).
8. **Master pipeline:** ingestion → Databricks notebook activities (bronze→silver→gold) with `dependsOn` Succeeded.
Linked services use **managed identity / Key Vault** references, never inline secrets.
