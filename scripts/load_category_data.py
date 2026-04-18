#!/usr/bin/env python3
"""Category data aggregation pipeline for NAEI 2024 PV data.

Reads naei2024pv_series / naei2024pv_values from SUPABASE_DB_URL,
aggregates by category definitions in naei_global_t_category on SBASE_CATDATA_DB_URL,
and writes wide-format results to naei_2024pv_t_category_data on SBASE_CATDATA_DB_URL.

Usage:
  python scripts/load_category_data.py
  python scripts/load_category_data.py --source-dsn <dsn> --target-dsn <dsn>
"""

from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

try:
    import psycopg
except ImportError:
    psycopg = None

FIRST_YEAR = 1970
LAST_YEAR = 2024
YEARS = list(range(FIRST_YEAR, LAST_YEAR + 1))


@dataclass(frozen=True)
class Category:
    id: int
    category_title: Optional[str]
    nfr_code: Optional[str]
    source_name: Optional[str]
    activity_name: Optional[str]

    def label(self) -> str:
        return self.category_title or f"category_id={self.id}"


@dataclass
class CategoryRunSummary:
    categories_loaded: int
    categories_with_data: int
    categories_without_data: List[str]
    rows_inserted: int
    unresolved_warnings: List[str]


def execute_values(cur: Any, query: str, rows: Sequence[Sequence[object]], page_size: int = 500) -> None:
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
            flat_params.extend(row)
        cur.execute(query.replace("VALUES %s", f"VALUES {placeholders}"), flat_params)


def require_psycopg() -> None:
    if psycopg is None:
        raise RuntimeError("psycopg is not installed. Install dependencies from requirements.txt")


def load_env_dsns(source_dsn_arg: Optional[str], target_dsn_arg: Optional[str]) -> Tuple[str, str]:
    if load_dotenv is not None:
        load_dotenv()
    source_dsn = source_dsn_arg or os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    target_dsn = target_dsn_arg or os.environ.get("SBASE_CATDATA_DB_URL")
    if not source_dsn:
        raise RuntimeError("Missing source DSN. Pass --source-dsn or set SUPABASE_DB_URL / DATABASE_URL")
    if not target_dsn:
        raise RuntimeError("Missing target DSN. Pass --target-dsn or set SBASE_CATDATA_DB_URL")
    return source_dsn, target_dsn


def split_tokens(value: Optional[str]) -> List[str]:
    if not value or not value.strip() or value.strip().upper() == "NULL":
        return []
    return [t.strip() for t in value.split(";") if t.strip() and t.strip().upper() != "NULL"]


def load_dimension_lookups(
    src_cur: Any,
) -> Tuple[Dict[str, int], Dict[str, int], Dict[str, int]]:
    src_cur.execute("SELECT nfr_code, id FROM naei_global_t_nfrcode WHERE nfr_code IS NOT NULL")
    nfr = {row[0].strip().lower(): int(row[1]) for row in src_cur.fetchall()}

    src_cur.execute("SELECT source_name, id FROM naei_global_t_sourcename WHERE source_name IS NOT NULL")
    source = {row[0].strip().lower(): int(row[1]) for row in src_cur.fetchall()}

    src_cur.execute("SELECT activity_name, id FROM naei_global_t_activityname WHERE activity_name IS NOT NULL")
    activity = {row[0].strip().lower(): int(row[1]) for row in src_cur.fetchall()}

    return nfr, source, activity


def resolve_tokens(
    tokens: List[str],
    lookup: Dict[str, int],
    dim_name: str,
    category_label: str,
    warnings: List[str],
) -> List[int]:
    ids: List[int] = []
    for token in tokens:
        resolved = lookup.get(token.lower())
        if resolved is None:
            warnings.append(f"{category_label}: {dim_name} token '{token}' not found in source DB")
        else:
            ids.append(resolved)
    return ids


def aggregate_category(
    src_cur: Any,
    cat: Category,
    nfr_lookup: Dict[str, int],
    source_lookup: Dict[str, int],
    activity_lookup: Dict[str, int],
    warnings: List[str],
) -> List[tuple]:
    nfr_tokens = split_tokens(cat.nfr_code)
    source_tokens = split_tokens(cat.source_name)
    activity_tokens = split_tokens(cat.activity_name)

    nfr_ids = resolve_tokens(nfr_tokens, nfr_lookup, "nfr_code", cat.label(), warnings)
    source_ids = resolve_tokens(source_tokens, source_lookup, "source_name", cat.label(), warnings)
    activity_ids = resolve_tokens(activity_tokens, activity_lookup, "activity_name", cat.label(), warnings)

    # Tokens specified but none resolved — category matches nothing in source DB
    if nfr_tokens and not nfr_ids:
        return []
    if source_tokens and not source_ids:
        return []
    if activity_tokens and not activity_ids:
        return []

    conditions = ["s.pollutant_id IS NOT NULL"]
    params: List[Any] = []

    if nfr_ids:
        conditions.append("s.nfr_group_id = ANY(%s)")
        params.append(nfr_ids)
    if source_ids:
        conditions.append("s.source_id = ANY(%s)")
        params.append(source_ids)
    if activity_ids:
        conditions.append("s.activity_id = ANY(%s)")
        params.append(activity_ids)

    year_sums = ",\n      ".join(
        f"SUM(v.metric_value) FILTER (WHERE v.reporting_year = {year})"
        for year in YEARS
    )
    where_clause = " AND ".join(conditions)

    src_cur.execute(
        f"""
        SELECT
          s.pollutant_id,
          {year_sums}
        FROM naei2024pv_series s
        JOIN naei2024pv_values v
          ON v.pv_series_id = s.pv_series_id
         AND v.metric_label = 'value'
         AND v.reporting_year BETWEEN {FIRST_YEAR} AND {LAST_YEAR}
        WHERE {where_clause}
        GROUP BY s.pollutant_id
        """,
        params,
    )
    return src_cur.fetchall()


def run_category_pipeline(source_dsn: str, target_dsn: str) -> CategoryRunSummary:
    require_psycopg()

    categories_without_data: List[str] = []
    all_warnings: List[str] = []
    buffer: List[tuple] = []
    categories: List[Category] = []

    with psycopg.connect(source_dsn) as src_conn, psycopg.connect(target_dsn) as tgt_conn:
        with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:

            print("Loading dimension lookups from source DB...")
            nfr_lookup, source_lookup, activity_lookup = load_dimension_lookups(src_cur)
            print(
                f"  {len(nfr_lookup)} NFR codes, "
                f"{len(source_lookup)} sources, "
                f"{len(activity_lookup)} activities"
            )

            print("Loading categories from target DB...")
            tgt_cur.execute(
                """
                SELECT id, category_title, nfr_code, source_name, activity_name
                FROM naei_global_t_category
                ORDER BY id
                """
            )
            categories = [
                Category(
                    id=int(row[0]),
                    category_title=row[1],
                    nfr_code=row[2],
                    source_name=row[3],
                    activity_name=row[4],
                )
                for row in tgt_cur.fetchall()
            ]
            print(f"  {len(categories)} categories to process\n")

            for i, cat in enumerate(categories, 1):
                cat_warnings: List[str] = []
                rows = aggregate_category(
                    src_cur, cat, nfr_lookup, source_lookup, activity_lookup, cat_warnings
                )
                all_warnings.extend(cat_warnings)

                if not rows:
                    categories_without_data.append(cat.label())
                    print(f"  [{i:>3}/{len(categories)}] {cat.label()!r}: no data")
                else:
                    print(f"  [{i:>3}/{len(categories)}] {cat.label()!r}: {len(rows)} pollutants")
                    for row in rows:
                        buffer.append((cat.id,) + tuple(row))

            print(f"\nTruncating naei_2024pv_t_category_data...")
            tgt_cur.execute("TRUNCATE TABLE naei_2024pv_t_category_data RESTART IDENTITY")

            if buffer:
                print(f"Inserting {len(buffer)} rows...")
                year_cols = ", ".join(f"f{y}" for y in YEARS)
                execute_values(
                    tgt_cur,
                    f"INSERT INTO naei_2024pv_t_category_data (category_id, pollutant_id, {year_cols}) VALUES %s",
                    buffer,
                )

            tgt_conn.commit()
            print("Committed.")

    return CategoryRunSummary(
        categories_loaded=len(categories),
        categories_with_data=len(categories) - len(categories_without_data),
        categories_without_data=categories_without_data,
        rows_inserted=len(buffer),
        unresolved_warnings=all_warnings,
    )


def print_summary(summary: CategoryRunSummary) -> None:
    print("\nSummary:")
    print(f"  Categories processed:    {summary.categories_loaded}")
    print(f"  Categories with data:    {summary.categories_with_data}")
    print(f"  Categories without data: {len(summary.categories_without_data)}")
    print(f"  Rows inserted:           {summary.rows_inserted}")

    if summary.categories_without_data:
        print("\nCategories with no matching data in source:")
        for label in summary.categories_without_data:
            print(f"  - {label}")

    if summary.unresolved_warnings:
        print("\nUnresolved token warnings:")
        for warning in summary.unresolved_warnings:
            print(f"  - {warning}")


def command_load(args: argparse.Namespace) -> int:
    source_dsn, target_dsn = load_env_dsns(
        getattr(args, "source_dsn", None),
        getattr(args, "target_dsn", None),
    )
    summary = run_category_pipeline(source_dsn=source_dsn, target_dsn=target_dsn)
    print_summary(summary)
    return 0


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Aggregate NAEI 2024 PV data into naei_2024pv_t_category_data"
    )
    parser.add_argument("--source-dsn", help="Source DB DSN (overrides SUPABASE_DB_URL)")
    parser.add_argument("--target-dsn", help="Target DB DSN (overrides SBASE_CATDATA_DB_URL)")
    parser.set_defaults(func=command_load)

    args = parser.parse_args(argv)
    try:
        return int(args.func(args))
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
