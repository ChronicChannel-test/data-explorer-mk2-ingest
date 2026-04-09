# data-explorer-mk2-ingest

Local ingest utilities for NAEI datasets.

## NAEI 2024 PV (PivotTableViewer) local workflow

The 2024 PV ingest script lives at `scripts/load_naei_data.py` and only reads these workbook sheets:

- `dbLinkAQ`
- `dbLinkHM`
- `dbLinkPM`
- `dbLinkPOP`

Presentation/report tabs are ignored.

### 1) Install dependencies

```bash
python3 -m pip install -r requirements.txt
```

### 2) Configure DB connection

Set one of:

- `SUPABASE_DB_URL`
- `DATABASE_URL`

You can put this in `.env` for local runs.

### 3) Apply schema patch (once per DB)

Run:

- `schema/migrations/2026-04-09_naei2024pv_ingest_patch.sql`

This patch adds territory support for `naei2024pv_series`, missing FKs, and unique constraints needed for idempotent upserts.

### 4) Extract normalized CSVs from workbook

```bash
python3 scripts/load_naei_data.py extract-pv-xlsx \
  --xlsx "/Users/mikehinford/Dropbox/Apps/github-data-explorer-mk2/2026/PivotTableViewer_naei24_AQ_2026_02_26.xlsx" \
  --output-dir "/Users/mikehinford/Dropbox/Apps/github-data-explorer-mk2/2026/extracted-pv" \
  --dataset-prefix NAEI2024pv
```

Output files:

- `NAEI2024pv_AQ_dblinkaq.csv`
- `NAEI2024pv_HM_dblinkhm.csv`
- `NAEI2024pv_PM_dblinkpm.csv`
- `NAEI2024pv_POP_dblinkpop.csv`

Normalized columns:

- `extracted_at`
- `source_sheet`
- `dataset_prefix`
- `territory_name`
- `pollutant`
- `reporting_year`
- `emission_unit`
- `source_name`
- `activity_name`
- `emission_value`
- `nfr_code`

### 5) Load normalized CSVs into Supabase/Postgres

```bash
python3 scripts/load_naei_data.py load-pv \
  --path "/Users/mikehinford/Dropbox/Apps/github-data-explorer-mk2/2026/extracted-pv" \
  --dataset-prefix NAEI2024pv \
  --source-url "PivotTableViewer_naei24_AQ_2026_02_26.xlsx"
```

### 6) Run extract + load in one command

```bash
python3 scripts/load_naei_data.py run-pv-ingest \
  --xlsx "/Users/mikehinford/Dropbox/Apps/github-data-explorer-mk2/2026/PivotTableViewer_naei24_AQ_2026_02_26.xlsx" \
  --output-dir "/Users/mikehinford/Dropbox/Apps/github-data-explorer-mk2/2026/extracted-pv" \
  --dataset-prefix NAEI2024pv \
  --source-url "PivotTableViewer_naei24_AQ_2026_02_26.xlsx"
```

## Notes

- `dataset_file.file_name` stores the CSV filename.
- `dataset_file.extracted_at` uses CSV file timestamp.
- Row-level `extracted_at` in normalized CSV comes from workbook `time-stamp` (with extraction run timestamp fallback if missing).
- Re-runs are idempotent when schema patch constraints are applied.
