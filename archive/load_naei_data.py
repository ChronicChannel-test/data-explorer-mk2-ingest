#!/usr/bin/env python3
"""Utilities for converting and loading NAEI CSV datasets into Supabase.

Usage examples:

1) Export PV workbook sheets to CSVs that follow the naming convention
   required by the loader (prefix_subtype_sheet.csv):

    python scripts/load_naei_data.py convert-pv \
        --xlsx "raw-data-csv_files/NAEI2023pv Data/PivotTableViewer_2025_AQ_v2.xlsx" \
        --prefix NAEI2023pv --subtype AQ \
        --output-dir raw-data-csv_files/NAEI2023pv Data/converted

2) Load every DS CSV in the source folder:

    python scripts/load_naei_data.py load-ds \
        --path raw-data-csv_files/NAEI2023ds_raw_data_load_csvs \
        --dsn "$SUPABASE_DB_URL"

3) Load converted PV CSVs (optionally overriding the inferred metric column):

    python scripts/load_naei_data.py load-pv \
        --path "raw-data-csv_files/NAEI2023pv Data/converted" \
        --metric-column Metric \
        --dsn "$SUPABASE_DB_URL"

Set SUPABASE_DB_URL (or DATABASE_URL) in your environment/.env so the loader
can connect directly to the Supabase Postgres instance.
"""

from __future__ import annotations

import argparse
import csv
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Protocol, Sequence, Set, Tuple

from argparse import ArgumentParser
import pandas as pd
import psycopg
from psycopg import Connection
from dotenv import load_dotenv


class SubparserAction(Protocol):
    def add_parser(self, name: str, *, help: Optional[str] = None, **kwargs: Any) -> ArgumentParser:
        ...

DATASET_DIM_COLUMNS = ["Gas", "NFR/CRT Group", "Source", "Activity", "Units"]
PREFIX_PATTERN = re.compile(r"^naei(\d{4})(ds|pv)$", re.IGNORECASE)


def execute_values(cur: Any, query: str, rows: Sequence[Sequence[object]], page_size: int = 1_000) -> None:
    """Minimal execute_values replacement to avoid psycopg extras dependency."""

    if not rows:
        return
    if "%s" not in query:
        raise ValueError("Query must include a VALUES %s placeholder for bulk expansion.")

    num_columns = len(rows[0])
    value_template = "(" + ",".join(["%s"] * num_columns) + ")"
    for start in range(0, len(rows), page_size):
        chunk = rows[start : start + page_size]
        placeholders = ", ".join([value_template] * len(chunk))
        flat_params: List[object] = []
        for row in chunk:
            if len(row) != num_columns:
                raise ValueError("All rows must have the same number of columns")
            flat_params.extend(row)
        expanded_query = query.replace("VALUES %s", f"VALUES {placeholders}")
        cur.execute(expanded_query, flat_params)


def normalize_dataset_prefix(value: str) -> str:
    """Force dataset prefix into the canonical NAEI####ds / NAEI####pv shape."""

    value = value.strip()
    match = PREFIX_PATTERN.match(value)
    if not match:
        return value
    year, suffix = match.groups()
    return f"NAEI{year}{suffix.lower()}"


def load_env_dsn(passed_dsn: Optional[str] = None) -> str:
    """Return the Postgres DSN, preferring the CLI flag over environment values."""

    if passed_dsn:
        return passed_dsn
    load_dotenv()
    dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "Missing connection string. Pass --dsn or set SUPABASE_DB_URL / DATABASE_URL."
        )
    return dsn


def slugify(value: str) -> str:
    """File-friendly slug: lowercase, alphanumeric + underscores only."""

    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "sheet"


def infer_dataset_tokens(
    path: Path, prefix_override: Optional[str], subtype_override: Optional[str]
) -> Tuple[str, str]:
    """Extract dataset prefix + subtype from a filename or use overrides."""

    if prefix_override and subtype_override:
        return prefix_override, subtype_override

    parts = path.stem.split("_")
    if len(parts) < 2:
        raise ValueError(
            f"Cannot infer dataset tokens from '{path.name}'. Use --prefix/--subtype."
        )
    prefix = prefix_override or parts[0]
    subtype = subtype_override or parts[1]
    return prefix, subtype


@dataclass
class DatasetMeta:
    dataset_prefix: str
    subtype: str
    file_name: str
    source_url: Optional[str]
    extracted_at: datetime


class DimensionCache:
    """Caches dimension IDs to avoid repeated round-trips."""

    def __init__(self) -> None:
        self.pollutant: Dict[str, int] = {}
        self.nfr: Dict[str, int] = {}
        self.source: Dict[str, int] = {}
        self.activity: Dict[str, int] = {}
        self.unit: Dict[str, int] = {}

    @staticmethod
    def normalize_value(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    def pollutant_id(self, cur: Any, name: str, pollutant_class: Optional[str]) -> int:
        normalized = self.normalize_value(name)
        if not normalized:
            raise ValueError("Pollutant name cannot be blank")
        key = normalized.lower()
        if key in self.pollutant:
            return self.pollutant[key]
        alias_match = self._pollutant_id_from_alias(cur, key)
        if alias_match is not None:
            self.pollutant[key] = alias_match
            return alias_match
        cur.execute(
            """
            INSERT INTO naei_global_t_pollutant (pollutant)
            VALUES (%s)
            ON CONFLICT (pollutant)
            DO UPDATE SET pollutant = EXCLUDED.pollutant
            RETURNING id
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM naei_global_t_pollutant WHERE pollutant = %s", (normalized,))
            row = cur.fetchone()
        pollutant_id = int(row[0])
        self._upsert_pollutant_alias(cur, normalized, pollutant_id)
        self.pollutant[key] = pollutant_id
        return pollutant_id

    def _pollutant_id_from_alias(self, cur: Any, alias_key: str) -> Optional[int]:
        if not alias_key:
            return None
        cur.execute(
            """
            SELECT pollutant_id
            FROM naei_global_t_pollutant_alias
            WHERE alias_key = %s
            """,
            (alias_key,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0])

    def _upsert_pollutant_alias(self, cur: Any, alias_name: str, pollutant_id: int) -> None:
        cur.execute(
            """
            INSERT INTO naei_global_t_pollutant_alias (alias_name, pollutant_id)
            VALUES (%s, %s)
            ON CONFLICT (alias_key)
            DO UPDATE SET pollutant_id = EXCLUDED.pollutant_id
            """,
            (alias_name, pollutant_id),
        )

    def nfr_id(self, cur: Any, code: Optional[str], description: Optional[str]) -> Optional[int]:
        code = self.normalize_value(code)
        if not code:
            return None
        key = code.lower()
        if key in self.nfr:
            return self.nfr[key]
        cur.execute(
            """
            INSERT INTO naei_global_t_nfrcode (nfr_code, description)
            VALUES (%s, %s)
            ON CONFLICT (nfr_code)
            DO UPDATE SET description = COALESCE(EXCLUDED.description, naei_global_t_nfrcode.description)
            RETURNING id
            """,
            (code, description),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM naei_global_t_nfrcode WHERE nfr_code = %s", (code,))
            row = cur.fetchone()
        nfr_id = int(row[0])
        self.nfr[key] = nfr_id
        return nfr_id

    def source_id(self, cur: Any, name: Optional[str]) -> Optional[int]:
        name = self.normalize_value(name)
        if not name:
            return None
        key = name.lower()
        if key in self.source:
            return self.source[key]
        cur.execute(
            """
            INSERT INTO naei_global_t_sourcename (source_name)
            VALUES (%s)
            ON CONFLICT (source_name) DO NOTHING
            RETURNING id
            """,
            (name,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM naei_global_t_sourcename WHERE source_name = %s", (name,))
            row = cur.fetchone()
        source_id = int(row[0])
        self.source[key] = source_id
        return source_id

    def activity_id(self, cur: Any, name: Optional[str]) -> Optional[int]:
        name = self.normalize_value(name)
        if not name:
            return None
        key = name.lower()
        if key in self.activity:
            return self.activity[key]
        cur.execute(
            """
            INSERT INTO naei_global_t_activityname (activity_name)
            VALUES (%s)
            ON CONFLICT (activity_name) DO NOTHING
            RETURNING id
            """,
            (name,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT id FROM naei_global_t_activityname WHERE activity_name = %s", (name,))
            row = cur.fetchone()
        activity_id = int(row[0])
        self.activity[key] = activity_id
        return activity_id

    def unit_id(self, cur: Any, name: Optional[str]) -> Optional[int]:
        name = self.normalize_value(name)
        if not name:
            return None
        key = name.lower()
        if key in self.unit:
            return self.unit[key]
        cur.execute(
            """
            INSERT INTO unit (unit_name)
            VALUES (%s)
            ON CONFLICT (unit_name) DO NOTHING
            RETURNING unit_id
            """,
            (name,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("SELECT unit_id FROM unit WHERE unit_name = %s", (name,))
            row = cur.fetchone()
        unit_id = row[0]
        self.unit[key] = unit_id
        return unit_id


class NAEILoader:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn
        self.dim_cache = DimensionCache()

    def ensure_dataset_file(self, cur: Any, meta: DatasetMeta) -> int:
        cur.execute(
            """
            INSERT INTO dataset_file (dataset_prefix, file_name, extracted_at, source_url)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (dataset_prefix, file_name)
            DO UPDATE SET extracted_at = EXCLUDED.extracted_at,
                          source_url = COALESCE(EXCLUDED.source_url, dataset_file.source_url)
            RETURNING dataset_file_id
            """,
            (
                meta.dataset_prefix,
                meta.file_name,
                meta.extracted_at,
                meta.source_url,
            ),
        )
        return cur.fetchone()[0]

    def upsert_ds_series(
        self,
        cur: Any,
        dataset_file_id: int,
        pollutant_id: int,
        nfr_group_id: Optional[int],
        source_id: Optional[int],
        activity_id: Optional[int],
    ) -> int:
        cur.execute(
            """
            INSERT INTO naei2023ds_series (
                dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id)
            DO UPDATE SET dataset_file_id = EXCLUDED.dataset_file_id
            RETURNING ds_series_id
            """,
            (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id),
        )
        return cur.fetchone()[0]

    def upsert_pv_series(
        self,
        cur: Any,
        dataset_file_id: int,
        pollutant_id: Optional[int],
        nfr_group_id: Optional[int],
        source_id: Optional[int],
        activity_id: Optional[int],
    ) -> int:
        cur.execute(
            """
            INSERT INTO naei2023pv_series (
                dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id)
            DO UPDATE SET dataset_file_id = EXCLUDED.dataset_file_id
            RETURNING pv_series_id
            """,
            (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id),
        )
        return cur.fetchone()[0]

    def load_ds_csv(
        self,
        path: Path,
        dataset_prefix: Optional[str] = None,
        subtype: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> None:
        meta = self._build_meta(path, dataset_prefix, subtype, source_url)
        print(f"Loading DS file: {path}")
        with self.conn.cursor() as cur, path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise ValueError(f"Missing header row in {path}")
            year_columns = self._extract_year_columns(fieldnames)
            if not year_columns:
                raise ValueError(f"No year columns found in {path}")
            dataset_file_id = self.ensure_dataset_file(cur, meta)
            pollutant_class = meta.subtype
            batch_rows: List[Tuple[int, int, float]] = []
            batch_size = 5_000
            for row in reader:
                pollutant_name = row.get("Pollutant") or row.get("Gas")
                if not pollutant_name:
                    continue
                pollutant_id = self.dim_cache.pollutant_id(cur, pollutant_name, pollutant_class)
                nfr_id = self.dim_cache.nfr_id(cur, row.get("NFR/CRT Group"), row.get("NFR/CRT Group"))
                source_id = self.dim_cache.source_id(cur, row.get("Source"))
                activity_id = self.dim_cache.activity_id(cur, row.get("Activity"))
                ds_series_id = self.upsert_ds_series(
                    cur,
                    dataset_file_id,
                    pollutant_id,
                    nfr_id,
                    source_id,
                    activity_id,
                )
                for year in year_columns:
                    raw = row.get(str(year), "").strip()
                    if not raw or raw == "-":
                        continue
                    try:
                        value = float(raw)
                    except ValueError:
                        continue
                    batch_rows.append((ds_series_id, year, value))
                if len(batch_rows) >= batch_size:
                    self._flush_ds_values(cur, batch_rows)
                    batch_rows.clear()
            if batch_rows:
                self._flush_ds_values(cur, batch_rows)
            self.conn.commit()
        print(f"Completed DS load for {path}")

    def load_pv_csv(
        self,
        path: Path,
        dataset_prefix: Optional[str] = None,
        subtype: Optional[str] = None,
        source_url: Optional[str] = None,
        metric_column: Optional[str] = None,
    ) -> None:
        meta = self._build_meta(path, dataset_prefix, subtype, source_url)
        print(f"Loading PV file: {path}")
        with self.conn.cursor() as cur, path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            fieldnames = reader.fieldnames
            if fieldnames is None:
                raise ValueError(f"Missing header row in {path}")
            year_columns = self._extract_year_columns(fieldnames)
            if not year_columns:
                raise ValueError(f"No year columns found in {path}")
            dataset_file_id = self.ensure_dataset_file(cur, meta)
            pollutant_class = meta.subtype
            metric_column = metric_column or self._guess_metric_column(fieldnames)
            batch_rows: List[Tuple[int, int, str, float]] = []
            batch_size = 5_000
            for row in reader:
                pollutant_name = row.get("Pollutant") or row.get("Gas")
                pollutant_id = (
                    self.dim_cache.pollutant_id(cur, pollutant_name, pollutant_class)
                    if pollutant_name
                    else None
                )
                nfr_id = self.dim_cache.nfr_id(cur, row.get("NFR/CRT Group"), row.get("NFR/CRT Group"))
                source_id = self.dim_cache.source_id(cur, row.get("Source"))
                activity_id = self.dim_cache.activity_id(cur, row.get("Activity"))
                pv_series_id = self.upsert_pv_series(
                    cur,
                    dataset_file_id,
                    pollutant_id,
                    nfr_id,
                    source_id,
                    activity_id,
                )
                metric_label = row.get(metric_column, "value") if metric_column else "value"
                metric_label = metric_label or "value"
                for year in year_columns:
                    raw = row.get(str(year), "").strip()
                    if not raw or raw == "-":
                        continue
                    try:
                        value = float(raw)
                    except ValueError:
                        continue
                    batch_rows.append((pv_series_id, year, metric_label, value))
                if len(batch_rows) >= batch_size:
                    self._flush_pv_values(cur, batch_rows)
                    batch_rows.clear()
            if batch_rows:
                self._flush_pv_values(cur, batch_rows)
            self.conn.commit()
        print(f"Completed PV load for {path}")

    def validate_lookup_tables(self, csv_paths: Sequence[Path]) -> int:
        dimension_values: Dict[str, Set[str]] = {
            "pollutant": set(),
            "nfr": set(),
            "source": set(),
            "activity": set(),
        }
        pollutant_unit_map: Dict[str, Set[str]] = defaultdict(set)
        pollutant_alias_display: Dict[str, str] = {}
        for csv_path in csv_paths:
            with csv_path.open("r", newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fieldnames = reader.fieldnames
                if fieldnames is None:
                    raise ValueError(f"Missing header row in {csv_path}")
                for row in reader:
                    pollutant_raw = row.get("Pollutant") or row.get("Gas")
                    pollutant_name = DimensionCache.normalize_value(pollutant_raw)
                    unit_raw_value = row.get("Units") or ""
                    if pollutant_name:
                        dimension_values["pollutant"].add(pollutant_name)
                        alias_key = pollutant_name.lower()
                        pollutant_alias_display.setdefault(alias_key, pollutant_raw or pollutant_name)
                        pollutant_unit_map[alias_key].add(unit_raw_value.strip())
                    nfr_value = DimensionCache.normalize_value(row.get("NFR/CRT Group"))
                    if nfr_value:
                        dimension_values["nfr"].add(nfr_value)
                    source_value = DimensionCache.normalize_value(row.get("Source"))
                    if source_value:
                        dimension_values["source"].add(source_value)
                    activity_value = DimensionCache.normalize_value(row.get("Activity"))
                    if activity_value:
                        dimension_values["activity"].add(activity_value)
                    # Units now come from pollutant metadata, so we only keep them
                    # for mismatch detection instead of checking a separate table.

        with self.conn.cursor() as cur:
            missing_pollutants = self._missing_pollutant_aliases(cur, dimension_values["pollutant"])
            missing_nfr = self._missing_lookup_values(cur, "naei_global_t_nfrcode", "nfr_code", dimension_values["nfr"])
            missing_sources = self._missing_lookup_values(cur, "naei_global_t_sourcename", "source_name", dimension_values["source"])
            missing_activities = self._missing_lookup_values(cur, "naei_global_t_activityname", "activity_name", dimension_values["activity"])
            unit_mismatches = self._pollutant_unit_mismatches(cur, pollutant_unit_map, pollutant_alias_display)

        summary: List[Tuple[str, Set[str]]] = []
        if missing_pollutants:
            summary.append(("Pollutant aliases", missing_pollutants))
        if missing_nfr:
            summary.append(("NFR codes", missing_nfr))
        if missing_sources:
            summary.append(("Sources", missing_sources))
        if missing_activities:
            summary.append(("Activities", missing_activities))
        if unit_mismatches:
            summary.append(("Pollutant unit mismatches", unit_mismatches))

        if summary:
            print("Missing lookup values detected:")
            for label, values in summary:
                print(f"{label}:")
                for value in sorted(values):
                    print(f"  - {value}")
        else:
            print("All referenced lookup values already exist.")

        return sum(len(values) for _, values in summary)

    def _missing_pollutant_aliases(self, cur: Any, values: Set[str]) -> Set[str]:
        if not values:
            return set()
        alias_keys = {value.lower() for value in values}
        cur.execute(
            """
            SELECT alias_key
            FROM naei_global_t_pollutant_alias
            WHERE alias_key = ANY(%s)
            """,
            (list(alias_keys),),
        )
        existing = {row[0] for row in cur.fetchall()}
        return {value for value in values if value.lower() not in existing}

    def _missing_lookup_values(self, cur: Any, table: str, column: str, values: Set[str]) -> Set[str]:
        if not values:
            return set()
        query = f"SELECT {column} FROM {table} WHERE {column} = ANY(%s)"
        cur.execute(query, (list(values),))
        existing = {row[0] for row in cur.fetchall()}
        return set(values) - existing

    def _pollutant_unit_mismatches(
        self,
        cur: Any,
        combos: Dict[str, Set[str]],
        alias_labels: Dict[str, str],
    ) -> Set[str]:
        if not combos:
            return set()
        alias_keys = list(combos.keys())
        cur.execute(
            """
            SELECT a.alias_key, p.emission_unit
            FROM naei_global_t_pollutant_alias a
            JOIN naei_global_t_pollutant p ON p.id = a.pollutant_id
            WHERE a.alias_key = ANY(%s)
            """,
            (alias_keys,),
        )
        canonical: Dict[str, Tuple[Optional[str], Optional[str]]] = {}
        for alias_key, emission_unit in cur.fetchall():
            canonical[alias_key] = (emission_unit, DimensionCache.normalize_value(emission_unit))

        descriptions: Set[str] = set()
        for alias_key, units in combos.items():
            raw_expected, normalized_expected = canonical.get(alias_key, (None, None))
            if normalized_expected is None:
                continue
            mismatching = sorted(
                {
                    unit if unit else "(missing)"
                    for unit in units
                    if DimensionCache.normalize_value(unit) != normalized_expected
                }
            )
            if mismatching:
                label = alias_labels.get(alias_key, alias_key)
                display_expected = raw_expected or normalized_expected
                descriptions.add(
                    f"{label}: expected unit '{display_expected}' but saw {', '.join(mismatching)}"
                )
        return descriptions

    def _flush_ds_values(self, cur: Any, rows: List[Tuple[int, int, float]]) -> None:
        execute_values(
            cur,
            """
            INSERT INTO naei2023ds_values (ds_series_id, reporting_year, emission_value)
            VALUES %s
            ON CONFLICT (ds_series_id, reporting_year)
            DO UPDATE SET emission_value = EXCLUDED.emission_value
            """,
            rows,
        )

    def _flush_pv_values(self, cur: Any, rows: List[Tuple[int, int, str, float]]) -> None:
        execute_values(
            cur,
            """
            INSERT INTO naei2023pv_values (pv_series_id, reporting_year, metric_label, metric_value)
            VALUES %s
            ON CONFLICT (pv_series_id, reporting_year, metric_label)
            DO UPDATE SET metric_value = EXCLUDED.metric_value
            """,
            rows,
        )

    @staticmethod
    def _extract_year_columns(fieldnames: Sequence[str]) -> List[int]:
        years: List[int] = []
        for name in fieldnames:
            if not name:
                continue
            name = name.strip()
            if name.isdigit():
                years.append(int(name))
        return years

    @staticmethod
    def _guess_metric_column(fieldnames: Sequence[str]) -> Optional[str]:
        candidates = {"metric", "metric_label", "indicator", "measure"}
        for name in fieldnames:
            if name and name.strip().lower() in candidates:
                return name
        return None

    @staticmethod
    def _build_meta(
        path: Path,
        dataset_prefix: Optional[str],
        subtype: Optional[str],
        source_url: Optional[str],
    ) -> DatasetMeta:
        prefix, inferred_subtype = infer_dataset_tokens(path, dataset_prefix, subtype)
        prefix = normalize_dataset_prefix(prefix)
        subtype_value = (subtype or inferred_subtype).upper()
        extracted_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        return DatasetMeta(
            dataset_prefix=prefix,
            subtype=subtype_value,
            file_name=path.name,
            source_url=source_url,
            extracted_at=extracted_at,
        )


def convert_pv_workbook(xlsx_path: Path, output_dir: Path, prefix: str, subtype: str) -> None:
    if not xlsx_path.exists():
        raise FileNotFoundError(xlsx_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    xl: Any = pd.ExcelFile(xlsx_path)
    sheet_names: List[str] = [str(name) for name in xl.sheet_names]
    for sheet_name in sheet_names:
        df = pd.DataFrame(xl.parse(sheet_name))
        if df.empty:
            continue
        slug = slugify(sheet_name)
        csv_name = f"{prefix}_{subtype}_{slug}.csv"
        csv_path = output_dir / csv_name
        df_writer: Any = df
        df_writer.to_csv(csv_path, index=False)
        print(f"Saved {csv_path.relative_to(output_dir.parent)}")


def gather_csv_paths(path: Path) -> List[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(p for p in path.rglob("*.csv"))
    raise FileNotFoundError(path)


def add_convert_parser(subparsers: SubparserAction) -> None:
    parser = subparsers.add_parser("convert-pv", help="Convert PV workbook sheets to CSVs")
    parser.add_argument("--xlsx", required=True, type=Path, help="Path to the .xlsx workbook")
    parser.add_argument("--output-dir", required=True, type=Path, help="Destination directory")
    parser.add_argument("--prefix", required=True, help="Dataset prefix (e.g. NAEI2023pv)")
    parser.add_argument("--subtype", required=True, help="Subtype label (e.g. AQ)")


def add_load_parser(subparsers: SubparserAction, name: str, help_text: str) -> None:
    parser = subparsers.add_parser(name, help=help_text)
    parser.add_argument("--path", required=True, type=Path, help="CSV file or directory")
    parser.add_argument("--dsn", help="Database connection string (optional)")
    parser.add_argument("--prefix", help="Override dataset prefix")
    parser.add_argument("--subtype", help="Override dataset subtype")
    parser.add_argument("--source-url", help="Optional source URL for metadata")
    if name == "load-pv":
        parser.add_argument(
            "--metric-column",
            help="Column that stores the PV metric label (default auto-detect)",
        )


def add_validate_parser(subparsers: SubparserAction) -> None:
    parser = subparsers.add_parser(
        "validate-lookups",
        help="Report CSV dimension values that are missing from lookup tables",
    )
    parser.add_argument("--path", required=True, type=Path, help="CSV file or directory")
    parser.add_argument("--dsn", help="Database connection string (optional)")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="NAEI loader and converter")
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_convert_parser(subparsers)
    add_load_parser(subparsers, "load-ds", "Load DS CSVs into Supabase")
    add_load_parser(subparsers, "load-pv", "Load PV CSVs into Supabase")
    add_validate_parser(subparsers)
    args = parser.parse_args(argv)

    if args.command == "convert-pv":
        convert_pv_workbook(args.xlsx, args.output_dir, args.prefix, args.subtype)
        return 0

    if args.command == "validate-lookups":
        dsn = load_env_dsn(args.dsn)
        csv_paths = gather_csv_paths(args.path)
        if not csv_paths:
            print(f"No CSV files found under {args.path}")
            return 1
        with psycopg.connect(dsn) as conn:
            loader = NAEILoader(conn)
            missing = loader.validate_lookup_tables(csv_paths)
        return 1 if missing else 0

    dsn = load_env_dsn(args.dsn)
    csv_paths = gather_csv_paths(args.path)
    if not csv_paths:
        print(f"No CSV files found under {args.path}")
        return 1

    with psycopg.connect(dsn) as conn:
        loader = NAEILoader(conn)
        for csv_path in csv_paths:
            if args.command == "load-ds":
                loader.load_ds_csv(
                    csv_path,
                    dataset_prefix=args.prefix,
                    subtype=args.subtype,
                    source_url=args.source_url,
                )
            else:
                loader.load_pv_csv(
                    csv_path,
                    dataset_prefix=args.prefix,
                    subtype=args.subtype,
                    source_url=args.source_url,
                    metric_column=args.metric_column,
                )
    return 0


if __name__ == "__main__":
    sys.exit(main())
