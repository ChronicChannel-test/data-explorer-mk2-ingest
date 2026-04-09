#!/usr/bin/env python3
"""Local extractor/loader for NAEI 2024 PV dbLink workbook data.

Commands:
- extract-pv-xlsx: read dbLinkAQ/HM/PM/POP sheets and write normalized CSVs
- load-pv: load normalized CSVs into Supabase/Postgres
- run-pv-ingest: run extract + load in one step
"""

from __future__ import annotations

import argparse
import csv
import math
import os
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Set, Tuple

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover - exercised in environments without dependency
    load_dotenv = None

try:
    import psycopg
    from psycopg import Connection
except ImportError:  # pragma: no cover - exercised in environments without dependency
    psycopg = None
    Connection = Any  # type: ignore[misc,assignment]


PREFIX_PATTERN = re.compile(r"^naei(\d{4})(ds|pv)$", re.IGNORECASE)
DEFAULT_OUTPUT_DIR = Path.home() / "naei-ingest-output"

TARGET_PV_SHEETS: Dict[str, str] = {
    "dbLinkAQ": "AQ",
    "dbLinkHM": "HM",
    "dbLinkPM": "PM",
    "dbLinkPOP": "POP",
}

EXPECTED_HEADERS: Dict[str, List[str]] = {
    "dbLinkAQ": [
        "time-stamp",
        "TerritoryName",
        "Pollutant",
        "Year",
        "Emission Unit",
        "SourceName",
        "ActivityName",
        "Emission",
        "NFRCode",
    ],
    "dbLinkHM": [
        "time-stamp",
        "TerritoryName",
        "Pollutant",
        "Year",
        "Emission Unit",
        "SourceName",
        "ActivityName",
        "Emission",
        "NFRCode",
    ],
    "dbLinkPM": [
        "time-stamp",
        "TerritoryName",
        "Particle Size",
        "Year",
        "Emission Unit",
        "SourceName",
        "ActivityName",
        "Emission",
        "NFRCode",
    ],
    "dbLinkPOP": [
        "time-stamp",
        "TerritoryName",
        "Pollutant",
        "Year",
        "Emission Unit",
        "SourceName",
        "ActivityName",
        "Emission",
        "NFRCode",
    ],
}

NORMALIZED_COLUMNS: List[str] = [
    "extracted_at",
    "source_sheet",
    "dataset_prefix",
    "territory_name",
    "pollutant",
    "reporting_year",
    "emission_unit",
    "source_name",
    "activity_name",
    "emission_value",
    "nfr_code",
]


class SubparserAction(Protocol):
    def add_parser(self, name: str, *, help: Optional[str] = None, **kwargs: Any) -> argparse.ArgumentParser:
        ...


@dataclass(frozen=True)
class DatasetMeta:
    dataset_prefix: str
    file_name: str
    source_url: Optional[str]
    extracted_at: datetime


@dataclass(frozen=True)
class NormalizedPVRow:
    extracted_at: str
    source_sheet: str
    dataset_prefix: str
    territory_name: str
    pollutant: str
    reporting_year: int
    emission_unit: Optional[str]
    source_name: str
    activity_name: str
    emission_value: float
    nfr_code: str

    def series_lookup_key(self) -> Tuple[str, str, str, str, str]:
        # Territory is part of series identity for 2024 PV.
        return (
            self.territory_name.lower(),
            self.pollutant.lower(),
            self.nfr_code.lower(),
            self.source_name.lower(),
            self.activity_name.lower(),
        )


@dataclass
class ExtractSheetSummary:
    sheet_name: str
    csv_path: Path
    rows_written: int = 0
    rows_skipped: int = 0
    skipped_reasons: Dict[str, int] = field(default_factory=lambda: defaultdict(int))


@dataclass
class ExtractRunSummary:
    workbook_path: Path
    dataset_prefix: str
    output_dir: Path
    run_timestamp: datetime
    sheets: List[ExtractSheetSummary]


@dataclass
class LoadFileSummary:
    csv_path: Path
    dataset_prefix: str
    dataset_file_id: int
    rows_read: int
    rows_loaded: int
    rows_skipped: int
    skipped_reasons: Dict[str, int]


@dataclass
class LoadRunSummary:
    csv_paths: List[Path]
    files: List[LoadFileSummary]
    unit_conflicts: List[str]
    lookup_delta: Dict[str, int]


def execute_values(cur: Any, query: str, rows: Sequence[Sequence[object]], page_size: int = 1000) -> None:
    """Minimal execute_values helper to keep dependencies small."""

    if not rows:
        return
    if "%s" not in query:
        raise ValueError("Query must include a VALUES %s placeholder")

    num_columns = len(rows[0])
    value_template = "(" + ",".join(["%s"] * num_columns) + ")"
    for start in range(0, len(rows), page_size):
        chunk = rows[start : start + page_size]
        placeholders = ", ".join([value_template] * len(chunk))
        flat_params: List[object] = []
        for row in chunk:
            if len(row) != num_columns:
                raise ValueError("All rows must have same column count")
            flat_params.extend(row)
        cur.execute(query.replace("VALUES %s", f"VALUES {placeholders}"), flat_params)


def normalize_dataset_prefix(value: str) -> str:
    value = value.strip()
    match = PREFIX_PATTERN.match(value)
    if not match:
        return value
    year, suffix = match.groups()
    return f"NAEI{year}{suffix.lower()}"


def require_pv_dataset_prefix(value: str) -> str:
    normalized = normalize_dataset_prefix(value)
    if not PREFIX_PATTERN.match(normalized) or not normalized.lower().endswith("pv"):
        raise ValueError(
            f"Invalid dataset prefix '{value}'. Expected NAEI####pv format."
        )
    return normalized


def load_env_dsn(passed_dsn: Optional[str] = None) -> str:
    if passed_dsn:
        return passed_dsn
    if load_dotenv is not None:
        load_dotenv()
    dsn = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("Missing DSN. Pass --dsn or set SUPABASE_DB_URL / DATABASE_URL")
    return dsn


def require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Install dependencies from requirements.txt")


def clean_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def parse_year(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        year = value
    elif isinstance(value, float):
        if not math.isfinite(value):
            return None
        year = int(value)
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith(".0"):
            text = text[:-2]
        if not text.isdigit():
            return None
        year = int(text)
    if year < 1900 or year > 2100:
        return None
    return year


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if not math.isfinite(numeric):
            return None
        return numeric
    text = str(value).strip()
    if not text or text == "-":
        return None
    text = text.replace(",", "")
    try:
        numeric = float(text)
    except ValueError:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def parse_excel_timestamp(value: Any) -> Optional[str]:
    """Convert Excel timestamp values to ISO8601 strings when possible."""

    if value is None:
        return None

    if isinstance(value, datetime):
        dt = value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
        return dt.isoformat()

    if isinstance(value, (int, float)):
        if not math.isfinite(float(value)):
            return None
        dt = (datetime(1899, 12, 30) + timedelta(days=float(value))).replace(tzinfo=timezone.utc)
        return dt.isoformat()

    text = str(value).strip()
    if not text:
        return None

    try:
        numeric = float(text)
    except ValueError:
        numeric = None

    if numeric is not None and math.isfinite(numeric):
        dt = (datetime(1899, 12, 30) + timedelta(days=float(numeric))).replace(tzinfo=timezone.utc)
        return dt.isoformat()

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).isoformat()
    except ValueError:
        return text


def validate_sheet_headers(sheet_name: str, headers: Sequence[Any]) -> None:
    expected = EXPECTED_HEADERS[sheet_name]
    normalized = [str(value).strip() if value is not None else "" for value in headers]
    if normalized[: len(expected)] != expected:
        found_preview = ", ".join(repr(v) for v in normalized[: len(expected)])
        expected_preview = ", ".join(repr(v) for v in expected)
        raise ValueError(
            f"Unexpected headers in {sheet_name}. "
            f"Expected [{expected_preview}] but found [{found_preview}]"
        )


def normalize_db_link_row(
    *,
    source_sheet: str,
    dataset_prefix: str,
    raw_row: Mapping[str, Any],
    fallback_extracted_at: str,
) -> Tuple[Optional[NormalizedPVRow], Optional[str]]:
    pollutant_col = "Particle Size" if source_sheet == "dbLinkPM" else "Pollutant"

    territory_name = clean_text(raw_row.get("TerritoryName"))
    pollutant = clean_text(raw_row.get(pollutant_col))
    reporting_year = parse_year(raw_row.get("Year"))
    source_name = clean_text(raw_row.get("SourceName"))
    activity_name = clean_text(raw_row.get("ActivityName"))
    nfr_code = clean_text(raw_row.get("NFRCode"))
    emission_value = parse_float(raw_row.get("Emission"))
    emission_unit = clean_text(raw_row.get("Emission Unit"))

    row_timestamp = parse_excel_timestamp(raw_row.get("time-stamp"))
    extracted_at = row_timestamp or fallback_extracted_at

    if not pollutant or reporting_year is None:
        return None, "missing_pollutant_or_year"
    if territory_name is None:
        return None, "missing_territory"
    if source_name is None:
        return None, "missing_source"
    if activity_name is None:
        return None, "missing_activity"
    if nfr_code is None:
        return None, "missing_nfr_code"
    if emission_value is None:
        return None, "missing_or_invalid_emission"

    normalized = NormalizedPVRow(
        extracted_at=extracted_at,
        source_sheet=source_sheet,
        dataset_prefix=dataset_prefix,
        territory_name=territory_name,
        pollutant=pollutant,
        reporting_year=reporting_year,
        emission_unit=emission_unit,
        source_name=source_name,
        activity_name=activity_name,
        emission_value=emission_value,
        nfr_code=nfr_code,
    )
    return normalized, None


def normalized_row_to_csv(row: NormalizedPVRow) -> Dict[str, str]:
    return {
        "extracted_at": row.extracted_at,
        "source_sheet": row.source_sheet,
        "dataset_prefix": row.dataset_prefix,
        "territory_name": row.territory_name,
        "pollutant": row.pollutant,
        "reporting_year": str(row.reporting_year),
        "emission_unit": row.emission_unit or "",
        "source_name": row.source_name,
        "activity_name": row.activity_name,
        "emission_value": format(row.emission_value, ".15g"),
        "nfr_code": row.nfr_code,
    }


def parse_normalized_csv_row(
    raw_row: Mapping[str, str],
    dataset_prefix_override: Optional[str],
) -> Tuple[Optional[NormalizedPVRow], Optional[str]]:
    for col in NORMALIZED_COLUMNS:
        if col not in raw_row:
            return None, f"missing_column_{col}"

    dataset_prefix = require_pv_dataset_prefix(dataset_prefix_override) if dataset_prefix_override else clean_text(raw_row.get("dataset_prefix"))
    if not dataset_prefix:
        return None, "missing_dataset_prefix"
    if not PREFIX_PATTERN.match(dataset_prefix) or not dataset_prefix.lower().endswith("pv"):
        return None, "invalid_dataset_prefix"

    territory_name = clean_text(raw_row.get("territory_name"))
    pollutant = clean_text(raw_row.get("pollutant"))
    source_name = clean_text(raw_row.get("source_name"))
    activity_name = clean_text(raw_row.get("activity_name"))
    nfr_code = clean_text(raw_row.get("nfr_code"))
    reporting_year = parse_year(raw_row.get("reporting_year"))
    emission_value = parse_float(raw_row.get("emission_value"))
    source_sheet = clean_text(raw_row.get("source_sheet")) or "unknown"
    emission_unit = clean_text(raw_row.get("emission_unit"))

    if not pollutant or reporting_year is None:
        return None, "missing_pollutant_or_year"
    if territory_name is None:
        return None, "missing_territory"
    if source_name is None:
        return None, "missing_source"
    if activity_name is None:
        return None, "missing_activity"
    if nfr_code is None:
        return None, "missing_nfr_code"
    if emission_value is None:
        return None, "missing_or_invalid_emission"

    extracted_at = clean_text(raw_row.get("extracted_at")) or ""
    row = NormalizedPVRow(
        extracted_at=extracted_at,
        source_sheet=source_sheet,
        dataset_prefix=dataset_prefix,
        territory_name=territory_name,
        pollutant=pollutant,
        reporting_year=reporting_year,
        emission_unit=emission_unit,
        source_name=source_name,
        activity_name=activity_name,
        emission_value=emission_value,
        nfr_code=nfr_code,
    )
    return row, None


def gather_csv_paths(path: Path) -> List[Path]:
    if path.is_file():
        return [path]
    if path.is_dir():
        return sorted(item for item in path.rglob("*.csv") if item.is_file())
    raise FileNotFoundError(path)


def extract_pv_xlsx(xlsx_path: Path, output_dir: Path, dataset_prefix: str) -> ExtractRunSummary:
    if not xlsx_path.exists():
        raise FileNotFoundError(xlsx_path)

    normalized_prefix = require_pv_dataset_prefix(dataset_prefix)

    try:
        from openpyxl import load_workbook
    except ImportError as exc:  # pragma: no cover - dependency-driven
        raise RuntimeError("openpyxl is not installed. Install dependencies from requirements.txt") from exc

    output_dir.mkdir(parents=True, exist_ok=True)
    run_timestamp = datetime.now(timezone.utc).replace(microsecond=0)
    fallback_timestamp = run_timestamp.isoformat()

    workbook = load_workbook(xlsx_path, read_only=True, data_only=True)
    missing_sheets = sorted(set(TARGET_PV_SHEETS) - set(workbook.sheetnames))
    if missing_sheets:
        raise ValueError(f"Workbook is missing required sheets: {', '.join(missing_sheets)}")

    summaries: List[ExtractSheetSummary] = []

    try:
        for sheet_name, subtype in TARGET_PV_SHEETS.items():
            worksheet = workbook[sheet_name]
            rows = worksheet.iter_rows(values_only=True)
            header_row = next(rows, None)
            if header_row is None:
                raise ValueError(f"{sheet_name} has no header row")

            validate_sheet_headers(sheet_name, header_row)
            header_names = [str(value).strip() if value is not None else "" for value in header_row]
            header_index = {name: idx for idx, name in enumerate(header_names) if name}

            csv_name = f"{normalized_prefix}_{subtype}_{sheet_name.lower()}.csv"
            csv_path = output_dir / csv_name
            sheet_summary = ExtractSheetSummary(sheet_name=sheet_name, csv_path=csv_path)

            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=NORMALIZED_COLUMNS)
                writer.writeheader()

                for values in rows:
                    raw_row = {
                        column: values[index] if index < len(values) else None
                        for column, index in header_index.items()
                    }
                    normalized_row, reason = normalize_db_link_row(
                        source_sheet=sheet_name,
                        dataset_prefix=normalized_prefix,
                        raw_row=raw_row,
                        fallback_extracted_at=fallback_timestamp,
                    )
                    if normalized_row is None:
                        sheet_summary.rows_skipped += 1
                        if reason:
                            sheet_summary.skipped_reasons[reason] += 1
                        continue
                    writer.writerow(normalized_row_to_csv(normalized_row))
                    sheet_summary.rows_written += 1

            summaries.append(sheet_summary)
    finally:
        workbook.close()

    return ExtractRunSummary(
        workbook_path=xlsx_path,
        dataset_prefix=normalized_prefix,
        output_dir=output_dir,
        run_timestamp=run_timestamp,
        sheets=summaries,
    )


class DimensionCache:
    """Dimension lookup cache with created/reused counters."""

    def __init__(self) -> None:
        self.pollutant: Dict[str, int] = {}
        self.pollutant_unit: Dict[int, Optional[str]] = {}
        self.pollutant_label: Dict[int, str] = {}
        self.nfr: Dict[str, int] = {}
        self.source: Dict[str, int] = {}
        self.activity: Dict[str, int] = {}
        self.unit: Dict[str, int] = {}
        self.unit_conflicts: Set[str] = set()
        self.stats: Dict[str, int] = defaultdict(int)

    @staticmethod
    def normalize_value(value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        return value or None

    def _track(self, name: str) -> None:
        self.stats[name] += 1

    def _pollutant_id_from_alias(self, cur: Any, alias_key: str) -> Optional[Tuple[int, Optional[str], Optional[str]]]:
        cur.execute(
            """
            SELECT p.id, p.emission_unit, p.pollutant
            FROM naei_global_t_pollutant_alias a
            JOIN naei_global_t_pollutant p ON p.id = a.pollutant_id
            WHERE a.alias_key = %s
            """,
            (alias_key,),
        )
        row = cur.fetchone()
        if row is None:
            return None
        return int(row[0]), self.normalize_value(row[1]), row[2]

    def _upsert_pollutant_alias(self, cur: Any, alias_name: str, pollutant_id: int) -> None:
        cur.execute(
            """
            INSERT INTO naei_global_t_pollutant_alias (alias_name, pollutant_id)
            VALUES (%s, %s)
            ON CONFLICT (alias_key)
            DO UPDATE SET pollutant_id = EXCLUDED.pollutant_id,
                          alias_name = EXCLUDED.alias_name
            """,
            (alias_name, pollutant_id),
        )

    def _apply_pollutant_unit_rule(
        self,
        cur: Any,
        pollutant_id: int,
        pollutant_label: str,
        incoming_unit: Optional[str],
    ) -> None:
        incoming = self.normalize_value(incoming_unit)
        existing = self.pollutant_unit.get(pollutant_id)
        if incoming is None:
            return

        if existing is None:
            cur.execute(
                """
                UPDATE naei_global_t_pollutant
                SET emission_unit = %s
                WHERE id = %s AND emission_unit IS NULL
                """,
                (incoming, pollutant_id),
            )
            if cur.rowcount:
                self.pollutant_unit[pollutant_id] = incoming
                return
            cur.execute("SELECT emission_unit FROM naei_global_t_pollutant WHERE id = %s", (pollutant_id,))
            row = cur.fetchone()
            self.pollutant_unit[pollutant_id] = self.normalize_value(row[0]) if row else incoming
            return

        if existing.lower() != incoming.lower():
            warning = (
                f"{pollutant_label}: keeping canonical unit '{existing}' "
                f"and ignoring conflicting unit '{incoming}'"
            )
            self.unit_conflicts.add(warning)

    def pollutant_id(self, cur: Any, name: str, emission_unit: Optional[str]) -> int:
        normalized = self.normalize_value(name)
        if not normalized:
            raise ValueError("Pollutant cannot be blank")
        key = normalized.lower()
        if key in self.pollutant:
            pollutant_id = self.pollutant[key]
            self._track("pollutant_cache_hit")
            self._apply_pollutant_unit_rule(cur, pollutant_id, normalized, emission_unit)
            return pollutant_id

        alias_row = self._pollutant_id_from_alias(cur, key)
        if alias_row is not None:
            pollutant_id, canonical_unit, canonical_label = alias_row
            self.pollutant[key] = pollutant_id
            self.pollutant_unit[pollutant_id] = canonical_unit
            self.pollutant_label[pollutant_id] = canonical_label or normalized
            self._track("pollutant_reused")
            self._upsert_pollutant_alias(cur, normalized, pollutant_id)
            self._apply_pollutant_unit_rule(cur, pollutant_id, canonical_label or normalized, emission_unit)
            return pollutant_id

        cur.execute(
            """
            SELECT id, emission_unit, pollutant
            FROM naei_global_t_pollutant
            WHERE lower(pollutant) = lower(%s)
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                """
                INSERT INTO naei_global_t_pollutant (pollutant, emission_unit)
                VALUES (%s, %s)
                RETURNING id, emission_unit, pollutant
                """,
                (normalized, self.normalize_value(emission_unit)),
            )
            row = cur.fetchone()
            self._track("pollutant_created")
        else:
            self._track("pollutant_reused")

        pollutant_id = int(row[0])
        canonical_unit = self.normalize_value(row[1])
        canonical_label = row[2] or normalized
        self.pollutant[key] = pollutant_id
        self.pollutant_unit[pollutant_id] = canonical_unit
        self.pollutant_label[pollutant_id] = canonical_label
        self._upsert_pollutant_alias(cur, normalized, pollutant_id)
        self._apply_pollutant_unit_rule(cur, pollutant_id, canonical_label, emission_unit)
        return pollutant_id

    def nfr_id(self, cur: Any, code: str) -> int:
        normalized = self.normalize_value(code)
        if not normalized:
            raise ValueError("NFR code cannot be blank")
        key = normalized.lower()
        if key in self.nfr:
            self._track("nfr_cache_hit")
            return self.nfr[key]

        cur.execute(
            """
            SELECT id
            FROM naei_global_t_nfrcode
            WHERE lower(nfr_code) = lower(%s)
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO naei_global_t_nfrcode (nfr_code, description) VALUES (%s, %s) RETURNING id",
                (normalized, normalized),
            )
            row = cur.fetchone()
            self._track("nfr_created")
        else:
            self._track("nfr_reused")

        nfr_id = int(row[0])
        self.nfr[key] = nfr_id
        return nfr_id

    def source_id(self, cur: Any, name: str) -> int:
        normalized = self.normalize_value(name)
        if not normalized:
            raise ValueError("Source cannot be blank")
        key = normalized.lower()
        if key in self.source:
            self._track("source_cache_hit")
            return self.source[key]

        cur.execute(
            """
            SELECT id
            FROM naei_global_t_sourcename
            WHERE lower(source_name) = lower(%s)
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO naei_global_t_sourcename (source_name) VALUES (%s) RETURNING id",
                (normalized,),
            )
            row = cur.fetchone()
            self._track("source_created")
        else:
            self._track("source_reused")

        source_id = int(row[0])
        self.source[key] = source_id
        return source_id

    def activity_id(self, cur: Any, name: str) -> int:
        normalized = self.normalize_value(name)
        if not normalized:
            raise ValueError("Activity cannot be blank")
        key = normalized.lower()
        if key in self.activity:
            self._track("activity_cache_hit")
            return self.activity[key]

        cur.execute(
            """
            SELECT id
            FROM naei_global_t_activityname
            WHERE lower(activity_name) = lower(%s)
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO naei_global_t_activityname (activity_name) VALUES (%s) RETURNING id",
                (normalized,),
            )
            row = cur.fetchone()
            self._track("activity_created")
        else:
            self._track("activity_reused")

        activity_id = int(row[0])
        self.activity[key] = activity_id
        return activity_id

    def unit_id(self, cur: Any, name: Optional[str]) -> Optional[int]:
        normalized = self.normalize_value(name)
        if not normalized:
            return None
        key = normalized.lower()
        if key in self.unit:
            self._track("unit_cache_hit")
            return self.unit[key]

        cur.execute(
            """
            SELECT unit_id
            FROM unit
            WHERE lower(unit_name) = lower(%s)
            LIMIT 1
            """,
            (normalized,),
        )
        row = cur.fetchone()
        if row is None:
            cur.execute("INSERT INTO unit (unit_name) VALUES (%s) RETURNING unit_id", (normalized,))
            row = cur.fetchone()
            self._track("unit_created")
        else:
            self._track("unit_reused")

        unit_id = int(row[0])
        self.unit[key] = unit_id
        return unit_id


class NAEIPVLoader:
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
            (meta.dataset_prefix, meta.file_name, meta.extracted_at, meta.source_url),
        )
        return int(cur.fetchone()[0])

    def upsert_pv_series(
        self,
        cur: Any,
        *,
        dataset_file_id: int,
        pollutant_id: int,
        nfr_group_id: int,
        source_id: int,
        activity_id: int,
        territory_name: str,
    ) -> int:
        cur.execute(
            """
            INSERT INTO naei2024pv_series (
                dataset_file_id,
                pollutant_id,
                nfr_group_id,
                source_id,
                activity_id,
                territory_name
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (
                dataset_file_id,
                pollutant_id,
                nfr_group_id,
                source_id,
                activity_id,
                territory_name
            )
            DO UPDATE SET dataset_file_id = EXCLUDED.dataset_file_id
            RETURNING pv_series_id
            """,
            (dataset_file_id, pollutant_id, nfr_group_id, source_id, activity_id, territory_name),
        )
        return int(cur.fetchone()[0])

    def _flush_values(self, cur: Any, rows: List[Tuple[int, int, str, float]]) -> None:
        execute_values(
            cur,
            """
            INSERT INTO naei2024pv_values (pv_series_id, reporting_year, metric_label, metric_value)
            VALUES %s
            ON CONFLICT (pv_series_id, reporting_year, metric_label)
            DO UPDATE SET metric_value = EXCLUDED.metric_value
            """,
            rows,
            page_size=5000,
        )

    def load_pv_csv(
        self,
        csv_path: Path,
        *,
        dataset_prefix_override: Optional[str] = None,
        source_url: Optional[str] = None,
    ) -> LoadFileSummary:
        extracted_at = datetime.fromtimestamp(csv_path.stat().st_mtime, tz=timezone.utc)
        rows_read = 0
        rows_loaded = 0
        rows_skipped = 0
        skipped_reasons: Dict[str, int] = defaultdict(int)

        with self.conn.cursor() as cur, csv_path.open("r", newline="", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            if reader.fieldnames is None:
                raise ValueError(f"Missing header row in {csv_path}")
            missing_cols = [col for col in NORMALIZED_COLUMNS if col not in reader.fieldnames]
            if missing_cols:
                raise ValueError(f"{csv_path.name} is missing required columns: {', '.join(missing_cols)}")

            dataset_file_id: Optional[int] = None
            active_prefix: Optional[str] = require_pv_dataset_prefix(dataset_prefix_override) if dataset_prefix_override else None
            batch_rows: List[Tuple[int, int, str, float]] = []

            for raw_row in reader:
                rows_read += 1
                normalized_row, reason = parse_normalized_csv_row(raw_row, dataset_prefix_override)
                if normalized_row is None:
                    rows_skipped += 1
                    if reason:
                        skipped_reasons[reason] += 1
                    continue

                if active_prefix is None:
                    active_prefix = require_pv_dataset_prefix(normalized_row.dataset_prefix)

                if dataset_file_id is None:
                    meta = DatasetMeta(
                        dataset_prefix=active_prefix,
                        file_name=csv_path.name,
                        source_url=source_url,
                        extracted_at=extracted_at,
                    )
                    dataset_file_id = self.ensure_dataset_file(cur, meta)

                pollutant_id = self.dim_cache.pollutant_id(cur, normalized_row.pollutant, normalized_row.emission_unit)
                nfr_id = self.dim_cache.nfr_id(cur, normalized_row.nfr_code)
                source_id = self.dim_cache.source_id(cur, normalized_row.source_name)
                activity_id = self.dim_cache.activity_id(cur, normalized_row.activity_name)
                self.dim_cache.unit_id(cur, normalized_row.emission_unit)

                pv_series_id = self.upsert_pv_series(
                    cur,
                    dataset_file_id=dataset_file_id,
                    pollutant_id=pollutant_id,
                    nfr_group_id=nfr_id,
                    source_id=source_id,
                    activity_id=activity_id,
                    territory_name=normalized_row.territory_name,
                )

                batch_rows.append((pv_series_id, normalized_row.reporting_year, "value", normalized_row.emission_value))
                rows_loaded += 1

                if len(batch_rows) >= 5000:
                    self._flush_values(cur, batch_rows)
                    batch_rows.clear()

            if batch_rows:
                self._flush_values(cur, batch_rows)

            if dataset_file_id is None:
                raise ValueError(f"{csv_path.name} produced no valid rows to load")

            self.conn.commit()

        return LoadFileSummary(
            csv_path=csv_path,
            dataset_prefix=active_prefix or require_pv_dataset_prefix(dataset_prefix_override or "NAEI2024pv"),
            dataset_file_id=dataset_file_id,
            rows_read=rows_read,
            rows_loaded=rows_loaded,
            rows_skipped=rows_skipped,
            skipped_reasons=dict(sorted(skipped_reasons.items())),
        )


def run_load_pv(
    *,
    path: Path,
    dsn: str,
    source_url: Optional[str],
    dataset_prefix_override: Optional[str],
) -> LoadRunSummary:
    csv_paths = gather_csv_paths(path)
    if not csv_paths:
        raise RuntimeError(f"No CSV files found under {path}")
    return run_load_pv_paths(
        csv_paths=csv_paths,
        dsn=dsn,
        source_url=source_url,
        dataset_prefix_override=dataset_prefix_override,
    )


def run_load_pv_paths(
    *,
    csv_paths: Sequence[Path],
    dsn: str,
    source_url: Optional[str],
    dataset_prefix_override: Optional[str],
) -> LoadRunSummary:
    require_psycopg()
    with psycopg.connect(dsn) as conn:
        loader = NAEIPVLoader(conn)
        files: List[LoadFileSummary] = []
        before_stats = dict(loader.dim_cache.stats)
        before_conflicts = set(loader.dim_cache.unit_conflicts)

        for csv_path in csv_paths:
            files.append(
                loader.load_pv_csv(
                    csv_path,
                    dataset_prefix_override=dataset_prefix_override,
                    source_url=source_url,
                )
            )

        unit_conflicts = sorted(loader.dim_cache.unit_conflicts - before_conflicts)
        lookup_delta: Dict[str, int] = {}
        for key, after_value in loader.dim_cache.stats.items():
            lookup_delta[key] = after_value - before_stats.get(key, 0)

    return LoadRunSummary(
        csv_paths=csv_paths,
        files=files,
        unit_conflicts=unit_conflicts,
        lookup_delta=dict(sorted(lookup_delta.items())),
    )


def print_extract_summary(summary: ExtractRunSummary) -> None:
    print(f"Workbook: {summary.workbook_path}")
    print(f"Dataset prefix: {summary.dataset_prefix}")
    print(f"Output directory: {summary.output_dir}")
    print(f"Extraction run timestamp: {summary.run_timestamp.isoformat()}")
    print("Extracted CSVs:")
    for sheet in summary.sheets:
        print(
            f"- {sheet.sheet_name}: {sheet.csv_path.name} "
            f"(written={sheet.rows_written}, skipped={sheet.rows_skipped})"
        )
        for reason, count in sorted(sheet.skipped_reasons.items()):
            print(f"  skip[{reason}]={count}")


def print_load_summary(summary: LoadRunSummary) -> None:
    total_read = sum(item.rows_read for item in summary.files)
    total_loaded = sum(item.rows_loaded for item in summary.files)
    total_skipped = sum(item.rows_skipped for item in summary.files)

    print("Load summary:")
    for file_summary in summary.files:
        print(
            f"- {file_summary.csv_path.name}: "
            f"dataset_file_id={file_summary.dataset_file_id}, "
            f"rows_read={file_summary.rows_read}, "
            f"rows_loaded={file_summary.rows_loaded}, "
            f"rows_skipped={file_summary.rows_skipped}"
        )
        for reason, count in file_summary.skipped_reasons.items():
            print(f"  skip[{reason}]={count}")

    print(
        f"Totals: files={len(summary.files)}, rows_read={total_read}, "
        f"rows_loaded={total_loaded}, rows_skipped={total_skipped}"
    )

    if summary.lookup_delta:
        print("Lookup activity:")
        for key, value in summary.lookup_delta.items():
            if value:
                print(f"- {key}: {value}")

    if summary.unit_conflicts:
        print("Pollutant-unit conflicts (ingest continued):")
        for warning in summary.unit_conflicts:
            print(f"- {warning}")


def command_extract(args: argparse.Namespace) -> int:
    summary = extract_pv_xlsx(
        xlsx_path=args.xlsx,
        output_dir=args.output_dir,
        dataset_prefix=args.dataset_prefix,
    )
    print_extract_summary(summary)
    return 0


def command_load(args: argparse.Namespace) -> int:
    dsn = load_env_dsn(args.dsn)
    summary = run_load_pv(
        path=args.path,
        dsn=dsn,
        source_url=args.source_url,
        dataset_prefix_override=args.dataset_prefix,
    )
    print_load_summary(summary)
    return 0


def command_run(args: argparse.Namespace) -> int:
    extract_summary = extract_pv_xlsx(
        xlsx_path=args.xlsx,
        output_dir=args.output_dir,
        dataset_prefix=args.dataset_prefix,
    )
    print_extract_summary(extract_summary)

    dsn = load_env_dsn(args.dsn)
    load_summary = run_load_pv_paths(
        csv_paths=[sheet.csv_path for sheet in extract_summary.sheets],
        dsn=dsn,
        source_url=args.source_url,
        dataset_prefix_override=args.dataset_prefix,
    )
    print_load_summary(load_summary)
    return 0


def add_extract_parser(subparsers: SubparserAction) -> None:
    parser = subparsers.add_parser(
        "extract-pv-xlsx",
        help="Extract dbLink sheets from PivotTableViewer workbook into normalized CSVs",
    )
    parser.add_argument("--xlsx", required=True, type=Path, help="Path to PivotTableViewer workbook")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for normalized CSVs (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument(
        "--dataset-prefix",
        required=True,
        help="Dataset prefix (expected NAEI2024pv)",
    )
    parser.set_defaults(func=command_extract)


def add_load_parser(subparsers: SubparserAction) -> None:
    parser = subparsers.add_parser("load-pv", help="Load normalized PV CSV(s) into Supabase/Postgres")
    parser.add_argument("--path", required=True, type=Path, help="CSV file or directory containing CSV files")
    parser.add_argument("--dsn", help="Database DSN (optional if SUPABASE_DB_URL/DATABASE_URL set)")
    parser.add_argument("--source-url", help="Optional source URL to store in dataset_file")
    parser.add_argument("--dataset-prefix", help="Override dataset_prefix from CSV rows")
    parser.set_defaults(func=command_load)


def add_run_parser(subparsers: SubparserAction) -> None:
    parser = subparsers.add_parser("run-pv-ingest", help="Extract + load PV workbook in one command")
    parser.add_argument("--xlsx", required=True, type=Path, help="Path to PivotTableViewer workbook")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help=f"Output directory for normalized CSVs (default: {DEFAULT_OUTPUT_DIR})",
    )
    parser.add_argument("--dsn", help="Database DSN (optional if SUPABASE_DB_URL/DATABASE_URL set)")
    parser.add_argument("--dataset-prefix", required=True, help="Dataset prefix (expected NAEI2024pv)")
    parser.add_argument("--source-url", help="Optional source URL to store in dataset_file")
    parser.set_defaults(func=command_run)


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="NAEI 2024 PV extractor/loader")
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_extract_parser(subparsers)
    add_load_parser(subparsers)
    add_run_parser(subparsers)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:  # pragma: no cover - CLI top-level handler
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
