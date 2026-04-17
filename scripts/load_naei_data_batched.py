#!/usr/bin/env python3
"""Batched loader for normalized NAEI 2024 PV CSVs.

This script is intended for large files where one monolithic upsert statement
can run for too long or spill heavily to temp storage.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional, Sequence

from load_naei_data import gather_csv_paths, load_env_dsn, parse_year, require_pv_dataset_prefix

try:
    import psycopg
except ImportError:  # pragma: no cover - dependency-driven
    psycopg = None


STAGE_DDL = """
DROP TABLE IF EXISTS _stg_pv_raw;
DROP TABLE IF EXISTS _stg_pv;

CREATE TEMP TABLE _stg_pv_raw (
  pollutant text,
  reporting_year integer,
  emission_unit text,
  source_name text,
  activity_name text,
  emission_value double precision,
  nfr_code text
);
"""

STAGE_COPY_SQL = """
COPY _stg_pv_raw (
  pollutant,
  reporting_year,
  emission_unit,
  source_name,
  activity_name,
  emission_value,
  nfr_code
) FROM STDIN
"""

STAGE_REQUIRED_COLUMNS = (
    "pollutant",
    "reporting_year",
    "emission_unit",
    "source_name",
    "activity_name",
    "emission_value",
    "nfr_code",
)

STAGE_NORMALIZE_SQL = """
CREATE TEMP TABLE _stg_pv AS
SELECT
  LOWER(NULLIF(btrim(pollutant), '')) AS pollutant_key,
  NULLIF(btrim(pollutant), '') AS pollutant_name,
  LOWER(NULLIF(btrim(nfr_code), '')) AS nfr_key,
  NULLIF(btrim(nfr_code), '') AS nfr_name,
  LOWER(NULLIF(btrim(source_name), '')) AS source_key,
  NULLIF(btrim(source_name), '') AS source_name,
  LOWER(NULLIF(btrim(activity_name), '')) AS activity_key,
  NULLIF(btrim(activity_name), '') AS activity_name,
  NULLIF(btrim(emission_unit), '') AS emission_unit,
  COALESCE(%(force_reporting_year)s::integer, reporting_year) AS reporting_year,
  emission_value
FROM _stg_pv_raw
WHERE COALESCE(%(force_reporting_year)s::integer, reporting_year) IS NOT NULL
  AND emission_value IS NOT NULL;
"""

STAGE_INDEX_SQL = """
CREATE INDEX ON _stg_pv (reporting_year);
CREATE INDEX ON _stg_pv (pollutant_key, nfr_key, source_key, activity_key, reporting_year);
"""

UPSERT_DATASET_FILE_SQL = """
INSERT INTO dataset_file (dataset_prefix, file_name, extracted_at, source_url)
VALUES (%s, %s, %s, %s)
ON CONFLICT (dataset_prefix, file_name)
DO UPDATE SET extracted_at = EXCLUDED.extracted_at,
              source_url = COALESCE(EXCLUDED.source_url, dataset_file.source_url)
RETURNING dataset_file_id
"""

UPSERT_DIMENSIONS_SQL = """
WITH nfr_norm AS (
  SELECT nfr_key AS lk, min(nfr_name) AS display
  FROM _stg_pv
  WHERE nfr_key IS NOT NULL
  GROUP BY 1
)
INSERT INTO naei_global_t_nfrcode (nfr_code, description)
SELECT n.display, n.display
FROM nfr_norm n
LEFT JOIN (
  SELECT lower(nfr_code) AS lk, min(id) AS id
  FROM naei_global_t_nfrcode
  GROUP BY 1
) t ON t.lk = n.lk
WHERE t.id IS NULL;

WITH src_norm AS (
  SELECT source_key AS lk, min(source_name) AS display
  FROM _stg_pv
  WHERE source_key IS NOT NULL
  GROUP BY 1
)
INSERT INTO naei_global_t_sourcename (source_name)
SELECT s.display
FROM src_norm s
LEFT JOIN (
  SELECT lower(source_name) AS lk, min(id) AS id
  FROM naei_global_t_sourcename
  GROUP BY 1
) t ON t.lk = s.lk
WHERE t.id IS NULL;

WITH act_norm AS (
  SELECT activity_key AS lk, min(activity_name) AS display
  FROM _stg_pv
  WHERE activity_key IS NOT NULL
  GROUP BY 1
)
INSERT INTO naei_global_t_activityname (activity_name)
SELECT a.display
FROM act_norm a
LEFT JOIN (
  SELECT lower(activity_name) AS lk, min(id) AS id
  FROM naei_global_t_activityname
  GROUP BY 1
) t ON t.lk = a.lk
WHERE t.id IS NULL;

WITH unit_norm AS (
  SELECT lower(emission_unit) AS lk, min(emission_unit) AS display
  FROM _stg_pv
  WHERE emission_unit IS NOT NULL
  GROUP BY 1
)
INSERT INTO unit (unit_name)
SELECT u.display
FROM unit_norm u
LEFT JOIN (
  SELECT lower(unit_name) AS lk, min(unit_id) AS unit_id
  FROM unit
  GROUP BY 1
) t ON t.lk = u.lk
WHERE t.unit_id IS NULL;

WITH pol_norm AS (
  SELECT pollutant_key AS lk, min(pollutant_name) AS display, min(emission_unit) AS unit_name
  FROM _stg_pv
  WHERE pollutant_key IS NOT NULL
  GROUP BY 1
)
INSERT INTO naei_global_t_pollutant (pollutant, emission_unit)
SELECT p.display, p.unit_name
FROM pol_norm p
LEFT JOIN (
  SELECT lower(pollutant) AS lk, min(id) AS id
  FROM naei_global_t_pollutant
  GROUP BY 1
) t ON t.lk = p.lk
WHERE t.id IS NULL;

WITH pol_norm AS (
  SELECT pollutant_key AS lk, min(emission_unit) AS unit_name
  FROM _stg_pv
  WHERE pollutant_key IS NOT NULL
  GROUP BY 1
)
UPDATE naei_global_t_pollutant t
SET emission_unit = p.unit_name
FROM pol_norm p
WHERE lower(t.pollutant) = p.lk
  AND t.id = (
    SELECT min(id) FROM naei_global_t_pollutant t2 WHERE lower(t2.pollutant) = p.lk
  )
  AND t.emission_unit IS NULL
  AND p.unit_name IS NOT NULL;
"""

UPSERT_POLLUTANT_ALIAS_SQL = """
WITH pol_norm AS (
  SELECT pollutant_key AS lk, min(pollutant_name) AS display
  FROM _stg_pv
  WHERE pollutant_key IS NOT NULL
  GROUP BY 1
),
pmap AS (
  SELECT lower(pollutant) AS lk, min(id) AS pollutant_id
  FROM naei_global_t_pollutant
  GROUP BY 1
)
INSERT INTO naei_global_t_pollutant_alias (alias_name, pollutant_id)
SELECT pnorm.display, pmap.pollutant_id
FROM pol_norm pnorm
JOIN pmap ON pmap.lk = pnorm.lk
ON CONFLICT (alias_key)
DO UPDATE SET pollutant_id = EXCLUDED.pollutant_id,
              alias_name = EXCLUDED.alias_name;
"""

UPSERT_SERIES_SQL = """
WITH pmap AS (
  SELECT lower(pollutant) AS lk, min(id) AS id
  FROM naei_global_t_pollutant
  GROUP BY 1
),
nmap AS (
  SELECT lower(nfr_code) AS lk, min(id) AS id
  FROM naei_global_t_nfrcode
  GROUP BY 1
),
smap AS (
  SELECT lower(source_name) AS lk, min(id) AS id
  FROM naei_global_t_sourcename
  GROUP BY 1
),
amap AS (
  SELECT lower(activity_name) AS lk, min(id) AS id
  FROM naei_global_t_activityname
  GROUP BY 1
),
combos AS (
  SELECT DISTINCT pollutant_key, nfr_key, source_key, activity_key
  FROM _stg_pv
  WHERE pollutant_key IS NOT NULL
    AND nfr_key IS NOT NULL
    AND source_key IS NOT NULL
    AND activity_key IS NOT NULL
),
existing AS (
  SELECT
    dataset_file_id,
    pollutant_id,
    nfr_group_id,
    source_id,
    activity_id,
    min(pv_series_id) AS pv_series_id
  FROM naei2024pv_series
  WHERE dataset_file_id = %(dataset_file_id)s
  GROUP BY 1,2,3,4,5
)
INSERT INTO naei2024pv_series (
  dataset_file_id,
  pollutant_id,
  nfr_group_id,
  source_id,
  activity_id
)
SELECT
  %(dataset_file_id)s,
  p.id,
  n.id,
  s.id,
  a.id
FROM combos c
JOIN pmap p ON p.lk = c.pollutant_key
JOIN nmap n ON n.lk = c.nfr_key
JOIN smap s ON s.lk = c.source_key
JOIN amap a ON a.lk = c.activity_key
LEFT JOIN existing e
  ON e.dataset_file_id = %(dataset_file_id)s
 AND e.pollutant_id = p.id
 AND e.nfr_group_id = n.id
 AND e.source_id = s.id
 AND e.activity_id = a.id
WHERE e.pv_series_id IS NULL;
"""

SELECT_YEARS_SQL = """
SELECT DISTINCT reporting_year
FROM _stg_pv
WHERE reporting_year IS NOT NULL
ORDER BY 1
"""

UPSERT_VALUES_BY_YEAR_SQL = """
WITH pmap AS (
  SELECT lower(pollutant) AS lk, min(id) AS id
  FROM naei_global_t_pollutant
  GROUP BY 1
),
nmap AS (
  SELECT lower(nfr_code) AS lk, min(id) AS id
  FROM naei_global_t_nfrcode
  GROUP BY 1
),
smap AS (
  SELECT lower(source_name) AS lk, min(id) AS id
  FROM naei_global_t_sourcename
  GROUP BY 1
),
amap AS (
  SELECT lower(activity_name) AS lk, min(id) AS id
  FROM naei_global_t_activityname
  GROUP BY 1
),
series_map AS (
  SELECT
    dataset_file_id,
    pollutant_id,
    nfr_group_id,
    source_id,
    activity_id,
    min(pv_series_id) AS pv_series_id
  FROM naei2024pv_series
  WHERE dataset_file_id = %(dataset_file_id)s
  GROUP BY 1,2,3,4,5
),
resolved AS (
  SELECT
    t.reporting_year,
    t.emission_value,
    p.id AS pollutant_id,
    n.id AS nfr_group_id,
    s.id AS source_id,
    a.id AS activity_id
  FROM _stg_pv t
  JOIN pmap p ON p.lk = t.pollutant_key
  JOIN nmap n ON n.lk = t.nfr_key
  JOIN smap s ON s.lk = t.source_key
  JOIN amap a ON a.lk = t.activity_key
  WHERE t.reporting_year = %(reporting_year)s
)
INSERT INTO naei2024pv_values (pv_series_id, reporting_year, metric_label, metric_value)
SELECT
  ser.pv_series_id,
  r.reporting_year,
  'value',
  r.emission_value
FROM resolved r
JOIN series_map ser
  ON ser.dataset_file_id = %(dataset_file_id)s
 AND ser.pollutant_id = r.pollutant_id
 AND ser.nfr_group_id = r.nfr_group_id
 AND ser.source_id = r.source_id
 AND ser.activity_id = r.activity_id
ON CONFLICT (pv_series_id, reporting_year, metric_label)
DO UPDATE SET metric_value = EXCLUDED.metric_value;
"""


@dataclass
class FileSummary:
    csv_path: Path
    dataset_file_id: int
    stage_rows: int
    years_processed: int


def require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Install dependencies from requirements.txt")


def parse_reporting_year_arg(value: str) -> int:
    year = parse_year(value)
    if year is None:
        raise argparse.ArgumentTypeError("Year must be an integer between 1900 and 2100")
    return year


def table_exists(cur: Any, fq_name: str) -> bool:
    cur.execute("SELECT to_regclass(%s)", (fq_name,))
    return cur.fetchone()[0] is not None


def copy_csv_into_stage(cur: Any, csv_path: Path, force_reporting_year: Optional[int]) -> int:
    cur.execute(STAGE_DDL)
    with csv_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"Missing header row in {csv_path}")
        missing = [col for col in STAGE_REQUIRED_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"{csv_path.name} is missing required columns: {', '.join(missing)}")
        with cur.copy(STAGE_COPY_SQL) as copy:
            for row in reader:
                copy.write_row([row.get(col) for col in STAGE_REQUIRED_COLUMNS])

    cur.execute(STAGE_NORMALIZE_SQL, {"force_reporting_year": force_reporting_year})
    cur.execute(STAGE_INDEX_SQL)
    cur.execute("ANALYZE _stg_pv")
    cur.execute("SELECT count(*) FROM _stg_pv")
    return int(cur.fetchone()[0])


def load_file_batched(
    conn: Any,
    *,
    csv_path: Path,
    dataset_prefix: str,
    source_url: Optional[str],
    years_per_commit: int,
    force_reporting_year: Optional[int],
) -> FileSummary:
    extracted_at = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            UPSERT_DATASET_FILE_SQL,
            (dataset_prefix, csv_path.name, extracted_at, source_url),
        )
        dataset_file_id = int(cur.fetchone()[0])
        stage_rows = copy_csv_into_stage(cur, csv_path, force_reporting_year)
        print(
            f"loading {csv_path.name}: dataset_file_id={dataset_file_id}, staging_rows={stage_rows}",
            flush=True,
        )

        cur.execute(UPSERT_DIMENSIONS_SQL)
        if table_exists(cur, "public.naei_global_t_pollutant_alias"):
            cur.execute(UPSERT_POLLUTANT_ALIAS_SQL)
        cur.execute(UPSERT_SERIES_SQL, {"dataset_file_id": dataset_file_id})
        conn.commit()

        cur.execute(SELECT_YEARS_SQL)
        years = [int(row[0]) for row in cur.fetchall()]
        years_done = 0
        for year in years:
            cur.execute(
                UPSERT_VALUES_BY_YEAR_SQL,
                {"dataset_file_id": dataset_file_id, "reporting_year": year},
            )
            years_done += 1
            print(f"  {csv_path.name}: year {year} upserted", flush=True)
            if years_done % years_per_commit == 0:
                conn.commit()

        conn.commit()
        print(f"loaded {csv_path.name}", flush=True)

    return FileSummary(
        csv_path=csv_path,
        dataset_file_id=dataset_file_id,
        stage_rows=stage_rows,
        years_processed=years_done,
    )


def run_batched_load(
    *,
    path: Path,
    dsn: str,
    dataset_prefix: str,
    source_url: Optional[str],
    years_per_commit: int,
    work_mem: str,
    force_reporting_year: Optional[int],
) -> List[FileSummary]:
    require_psycopg()
    csv_paths = gather_csv_paths(path)
    if not csv_paths:
        raise RuntimeError(f"No CSV files found under {path}")

    summaries: List[FileSummary] = []
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("SET statement_timeout = 0")
            cur.execute("SET lock_timeout = 0")
            cur.execute(f"SET work_mem = '{work_mem}'")

        for csv_path in csv_paths:
            summaries.append(
                load_file_batched(
                    conn,
                    csv_path=csv_path,
                    dataset_prefix=dataset_prefix,
                    source_url=source_url,
                    years_per_commit=years_per_commit,
                    force_reporting_year=force_reporting_year,
                )
            )

    return summaries


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batched loader for normalized NAEI2024pv CSVs")
    parser.add_argument("--path", required=True, type=Path, help="CSV file or directory containing CSV files")
    parser.add_argument("--dataset-prefix", required=True, help="Dataset prefix (expected NAEI2024pv)")
    parser.add_argument("--dsn", help="Database DSN (optional if SUPABASE_DB_URL/DATABASE_URL set)")
    parser.add_argument("--source-url", help="Optional source URL to store in dataset_file")
    parser.add_argument(
        "--force-reporting-year",
        type=parse_reporting_year_arg,
        help="Optional override for reporting_year while staging CSV rows",
    )
    parser.add_argument(
        "--years-per-commit",
        type=int,
        default=1,
        help="Commit frequency for value upserts (in reporting-year batches). Default: 1",
    )
    parser.add_argument(
        "--work-mem",
        default="256MB",
        help="Session work_mem value for this loader (e.g., 256MB, 512MB). Default: 256MB",
    )
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.years_per_commit < 1:
        parser.error("--years-per-commit must be >= 1")

    dataset_prefix = require_pv_dataset_prefix(args.dataset_prefix)
    dsn = load_env_dsn(args.dsn)

    summaries = run_batched_load(
        path=args.path,
        dsn=dsn,
        dataset_prefix=dataset_prefix,
        source_url=args.source_url,
        years_per_commit=args.years_per_commit,
        work_mem=args.work_mem,
        force_reporting_year=args.force_reporting_year,
    )

    print("Batched load summary:")
    for item in summaries:
        print(
            f"- {item.csv_path.name}: "
            f"dataset_file_id={item.dataset_file_id}, "
            f"stage_rows={item.stage_rows}, "
            f"years_processed={item.years_processed}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
