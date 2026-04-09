import unittest
from datetime import datetime

from scripts.load_naei_data import (
    EXPECTED_HEADERS,
    NormalizedPVRow,
    normalize_db_link_row,
    parse_excel_timestamp,
    parse_normalized_csv_row,
    validate_sheet_headers,
)


class NormalizePVRowTests(unittest.TestCase):
    def test_pm_maps_particle_size_to_pollutant(self) -> None:
        row, reason = normalize_db_link_row(
            source_sheet="dbLinkPM",
            dataset_prefix="NAEI2024pv",
            raw_row={
                "time-stamp": 45723.568148148152,
                "TerritoryName": "United Kingdom",
                "Particle Size": " PM2.5 ",
                "Year": "2023",
                "Emission Unit": "kt",
                "SourceName": "Road Transport",
                "ActivityName": "Cars",
                "Emission": "12.5",
                "NFRCode": "1A3b",
            },
            fallback_extracted_at="2026-04-09T00:00:00+00:00",
        )

        self.assertIsNone(reason)
        assert row is not None
        self.assertEqual(row.pollutant, "PM2.5")
        self.assertEqual(row.reporting_year, 2023)
        self.assertEqual(row.emission_value, 12.5)

    def test_invalid_missing_fields_are_skipped(self) -> None:
        row, reason = normalize_db_link_row(
            source_sheet="dbLinkAQ",
            dataset_prefix="NAEI2024pv",
            raw_row={
                "time-stamp": 45723.567997685182,
                "TerritoryName": "United Kingdom",
                "Pollutant": "NOx",
                "Year": "",
                "Emission Unit": "kt",
                "SourceName": "Road Transport",
                "ActivityName": "Cars",
                "Emission": "12.5",
                "NFRCode": "1A3b",
            },
            fallback_extracted_at="2026-04-09T00:00:00+00:00",
        )

        self.assertIsNone(row)
        self.assertEqual(reason, "missing_pollutant_or_year")

    def test_series_key_includes_territory(self) -> None:
        row = NormalizedPVRow(
            extracted_at="2025-03-07T13:38:13+00:00",
            source_sheet="dbLinkAQ",
            dataset_prefix="NAEI2024pv",
            territory_name="Gibraltar",
            pollutant="NOx",
            reporting_year=2023,
            emission_unit="kt",
            source_name="Road Transport",
            activity_name="Cars",
            emission_value=12.5,
            nfr_code="1A3b",
        )
        key = row.series_lookup_key()

        self.assertEqual(key[0], "gibraltar")
        self.assertEqual(len(key), 5)


class HeaderValidationTests(unittest.TestCase):
    def test_validate_expected_headers(self) -> None:
        validate_sheet_headers("dbLinkAQ", EXPECTED_HEADERS["dbLinkAQ"])

    def test_validate_headers_raises_for_unexpected_shape(self) -> None:
        bad_headers = list(EXPECTED_HEADERS["dbLinkAQ"])
        bad_headers[2] = "WrongPollutant"
        with self.assertRaises(ValueError):
            validate_sheet_headers("dbLinkAQ", bad_headers)


class TimestampAndParsingTests(unittest.TestCase):
    def test_parse_excel_timestamp_numeric_serial(self) -> None:
        iso_value = parse_excel_timestamp(45723.567997685182)
        assert iso_value is not None
        self.assertTrue(iso_value.startswith("2025-03-07T13:37"))

    def test_parse_excel_timestamp_datetime(self) -> None:
        iso_value = parse_excel_timestamp(datetime(2026, 4, 9, 12, 30, 0))
        assert iso_value is not None
        self.assertTrue(iso_value.startswith("2026-04-09T12:30:00"))

    def test_parse_normalized_csv_row_honors_prefix_override(self) -> None:
        row, reason = parse_normalized_csv_row(
            {
                "extracted_at": "2025-03-07T13:37:55+00:00",
                "source_sheet": "dbLinkAQ",
                "dataset_prefix": "WRONGPREFIX",
                "territory_name": "United Kingdom",
                "pollutant": "NOx",
                "reporting_year": "2023",
                "emission_unit": "kt",
                "source_name": "Road Transport",
                "activity_name": "Cars",
                "emission_value": "1.25",
                "nfr_code": "1A3b",
            },
            dataset_prefix_override="NAEI2024pv",
        )

        self.assertIsNone(reason)
        assert row is not None
        self.assertEqual(row.dataset_prefix, "NAEI2024pv")


if __name__ == "__main__":
    unittest.main()
