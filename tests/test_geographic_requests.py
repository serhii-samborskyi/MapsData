"""Census loader and request generation tests; all network access is mocked."""

import csv
import importlib.util
import io
import json
import socket
import sys
import tempfile
import unittest
import zipfile
import zlib
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from http.client import IncompleteRead
from pathlib import Path
from unittest.mock import Mock, patch
from urllib.error import HTTPError, URLError

import geographic_requests as geo


def archive(kind, rows=None, *, year=2025, delimiter="|", text=None, filename=None, bom=False):
    if text is None:
        buffer = io.StringIO(newline="")
        fields = ["USPS", "GEOID", "NAME"] + (["LSAD"] if kind == "place" else [])
        writer = csv.DictWriter(buffer, fieldnames=fields, delimiter=delimiter, lineterminator="\r\n")
        writer.writeheader()
        writer.writerows(rows)
        text = buffer.getvalue().encode("utf-8-sig" if bom else "utf-8")
    if isinstance(text, str):
        text = text.encode("utf-8")
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w", compression=zipfile.ZIP_STORED) as zipped:
        zipped.writestr(filename or f"{year}_Gaz_{kind}_national.txt", text)
    return result.getvalue()


class Response(io.BytesIO):
    def __init__(self, data, *, length=True):
        super().__init__(data)
        self.headers = {"Content-Length": str(len(data))} if length else {}


class GeographicRequestsTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.config = geo.DatasetConfig(cache_dir=Path(self.temp.name) / "cache")
        self.states = [
            {"USPS": "AL", "GEOID": "01", "NAME": "Alabama"},
            {"USPS": "IL", "GEOID": "17", "NAME": "Illinois"},
            {"USPS": "PR", "GEOID": "72", "NAME": "Puerto Rico"},
        ]
        # Synthetic duplicate-name records exercise GEOID preservation.
        self.places = [
            {"USPS": "AL", "GEOID": "0100100", "NAME": "Abanda CDP", "LSAD": "57"},
            {"USPS": "IL", "GEOID": "1700113", "NAME": "Abingdon city", "LSAD": "25"},
            {"USPS": "IL", "GEOID": "1700178", "NAME": "Adair CDP", "LSAD": "57"},
            {"USPS": "IL", "GEOID": "1701000", "NAME": "Example Village village", "LSAD": "47"},
            {"USPS": "IL", "GEOID": "1772000", "NAME": "Springfield city", "LSAD": "25"},
            {"USPS": "IL", "GEOID": "1772001", "NAME": "Springfield CDP", "LSAD": "57"},
            {"USPS": "PR", "GEOID": "7212345", "NAME": "Pe\u00f1uelas comunidad", "LSAD": "55"},
            {"USPS": "PR", "GEOID": "7212346", "NAME": "Other zona urbana", "LSAD": "62"},
        ]
        self.open = Mock(side_effect=self.fetch)
        opener = patch.object(geo, "build_opener", return_value=Mock(open=self.open))
        opener.start()
        self.addCleanup(opener.stop)
        guard = patch.object(socket, "create_connection", side_effect=AssertionError("Real network forbidden"))
        guard.start()
        self.addCleanup(guard.stop)

    def fetch(self, request, timeout):
        self.assertTrue(request.full_url.startswith(geo.SOURCE_ROOT + "/"))
        self.assertGreater(timeout, 0)
        self.assertLessEqual(timeout, 60)
        kind = "state" if "_state_" in request.full_url else "place"
        year = int(request.full_url.rsplit("/", 1)[-1][:4])
        return Response(archive(kind, self.states if kind == "state" else self.places, year=year))

    def catalog(self, state="IL", **kwargs):
        return geo.catalog(state, config=self.config, **kwargs)

    def generate(self, state="IL", template="Plumber {{city}}, {{state}}", **kwargs):
        return geo.generate_city_requests(state, template, config=self.config, **kwargs)

    def test_catalog_accepts_abbreviation_and_name_and_returns_provenance(self):
        first = self.catalog()
        second = self.catalog("  illinois  ")
        self.assertEqual(first, second)
        self.assertEqual(first["place_count"], 5)
        self.assertEqual(first["places"][0], {"city": "Abingdon", "state": "IL", "state_name": "Illinois",
                                               "geoid": "1700113", "census_name": "Abingdon city", "lsad": "25", "is_cdp": False})
        self.assertEqual(first["year"], 2025)
        self.assertEqual(first["source_url"], f"{geo.SOURCE_ROOT}/2025_Gazetteer/2025_Gaz_place_national.zip")
        self.assertEqual(first["provenance"]["sources"]["place"]["record_count"], len(self.places))
        for kind in ("place", "state"):
            for field in ("archive_sha256", "content_sha256"):
                self.assertRegex(first["provenance"]["sources"][kind][field], r"^[a-f0-9]{64}$")
        self.assertEqual(self.open.call_count, 2)

    def test_unique_queries_keep_all_geoids_and_counts(self):
        result = self.generate()
        self.assertEqual(result["requests"], ["Plumber Abingdon, IL", "Plumber Adair, IL",
                                              "Plumber Example Village, IL", "Plumber Springfield, IL"])
        self.assertEqual(result["request_count"], 4)
        self.assertEqual(result["request_metadata"][-1], {
            "request": "Plumber Springfield, IL", "geoids": ["1772000", "1772001"]})
        self.assertEqual(result["coverage"]["included_place_count"], 5)
        self.assertEqual(result["coverage"]["incorporated_place_count"], 3)
        self.assertEqual(result["coverage"]["cdp_count"], 2)
        self.assertEqual(result["coverage"]["duplicate_query_count"], 1)
        self.assertEqual(result["coverage"]["excluded_cdp_count"], 0)

    def test_incorporated_only_filter_and_all_cdp_state(self):
        result = self.generate(include_cdps=False)
        self.assertEqual(result["request_count"], 3)
        self.assertEqual(result["coverage"]["excluded_cdp_count"], 2)
        self.assertNotIn("Plumber Adair, IL", result["requests"])
        empty = self.generate("Puerto Rico", include_cdps=False)
        self.assertEqual(empty["request_count"], 0)
        self.assertEqual(empty["coverage"]["excluded_cdp_count"], 2)

    def test_pure_generation_does_not_fetch_or_mutate_supplied_catalog(self):
        data = self.catalog()
        before = deepcopy(data)
        self.open.reset_mock()
        with patch.object(geo, "catalog", side_effect=AssertionError("Pure generation must not load data")):
            first = self.generate(catalog_data=data)
            second = self.generate("Illinois", catalog_data=data)
        self.assertEqual(first, second)
        self.assertEqual(data, before)
        first["provenance"]["sources"]["place"]["record_count"] = 999
        self.assertEqual(data, before)
        self.open.assert_not_called()

    def test_state_names_come_from_dataset_and_leading_zero_ids_are_preserved(self):
        result = self.generate("alabama", "{{city}} {{state_name}} {{geoid}}")
        self.assertEqual(result["requests"], ["Abanda Alabama 0100100"])
        self.assertEqual(result["request_metadata"][0]["geoids"], ["0100100"])

    def test_unicode_names_and_tab_delimited_quoted_fields(self):
        self.places[1]["NAME"] = 'Quoted | "Name" city'

        def fetch(request, timeout):
            kind = "state" if "_state_" in request.full_url else "place"
            return Response(archive(kind, self.states if kind == "state" else self.places,
                                    delimiter="\t", bom=True))

        self.open.side_effect = fetch
        self.assertIn('Quoted | "Name"', [p["city"] for p in self.catalog()["places"]])
        result = self.generate("PR")
        self.assertIn("Plumber Pe\u00f1uelas, PR", result["requests"])

    def test_pipe_delimited_parser_respects_csv_quoting(self):
        self.places[1]["NAME"] = 'Quoted | "Name" city'
        self.assertIn('Quoted | "Name"', [p["city"] for p in self.catalog()["places"]])

    def test_suffix_removal_uses_lsad_and_preserves_balance_names(self):
        self.places[1]["NAME"] = "Kansas City city"
        self.places[2].update({"NAME": "Sample city (balance)", "LSAD": "00"})
        self.places[3].update({"NAME": "Sample city and borough", "LSAD": "53"})
        names = {p["city"] for p in self.catalog()["places"]}
        self.assertTrue({"Kansas City", "Sample city (balance)", "Sample"} <= names)

    def test_year_configuration_is_immutable_and_cached_separately(self):
        with self.assertRaises(FrozenInstanceError):
            self.config.year = 2024
        first = self.catalog()
        second = geo.catalog("IL", config=replace(self.config, year=2024))
        self.assertEqual(first["year"], 2025)
        self.assertEqual(second["year"], 2024)
        self.assertEqual(self.open.call_count, 4)
        self.assertEqual(len(list(self.config.cache_dir.glob("*.json"))), 2)

    def test_valid_cache_is_not_refreshed_or_mutated_by_caller(self):
        first = self.catalog()
        before = deepcopy(first)
        first["places"][0]["city"] = "changed"
        self.places[1]["NAME"] = "Provider changed city"
        self.open.side_effect = AssertionError("Valid cache must not fetch")
        self.assertEqual(self.catalog(), before)

    def test_invalid_input_is_rejected_before_network(self):
        for options in ({"state": ""}, {"state": None}, {"state": "IL\n"},
                        {"template": "No city"}, {"template": "{{city}} {{typo}}"},
                        {"template": "{{{city}}}"}, {"template": "{{ city.upper() }}"},
                        {"template": "{{city}} {{state"}, {"template": "{{city}}\n{{state}}"},
                        {"template": "x" * 2001}, {"include_cdps": "false"},
                        {"max_requests": True}, {"max_requests": 0}, {"max_requests": 20001}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                self.generate(**options)
        self.open.assert_not_called()

    def test_unknown_state_and_mismatched_supplied_catalog(self):
        data = self.catalog()
        with self.assertRaises(ValueError):
            self.catalog("Atlantis")
        with self.assertRaises(ValueError):
            self.generate("AL", catalog_data=data)
        with self.assertRaises(ValueError):
            geo.generate_city_requests("IL", "{{city}}", catalog_data=data,
                                       config=replace(self.config, year=2024))

    def test_request_limit_rejects_entire_expansion(self):
        with self.assertRaisesRegex(ValueError, "no partial expansion"):
            self.generate(max_requests=3)
        self.assertEqual(self.generate(max_requests=4)["request_count"], 4)

    def test_large_expansion_obeys_20000_limit(self):
        data = self.catalog()
        data["places"] = [{"city": f"Place {i:05}", "state": "IL", "state_name": "Illinois",
                           "geoid": str(1700000 + i), "is_cdp": False} for i in range(20001)]
        data["place_count"] = len(data["places"])
        with self.assertRaisesRegex(ValueError, "exceeds 20000"):
            self.generate(catalog_data=data)
        data["places"].pop()
        data["place_count"] -= 1
        self.assertEqual(self.generate(catalog_data=data)["request_count"], 20000)

    def test_generated_request_length_is_bounded(self):
        data = self.catalog()
        data["places"][0]["city"] = "Long place name " * 10
        with self.assertRaisesRegex(ValueError, "4000"):
            self.generate(template="{{city}} " * 200, catalog_data=data)

    def test_duplicate_queries_ignore_case_and_whitespace_but_keep_literal_text(self):
        data = self.catalog()
        data["places"][-1]["city"] = "SPRINGFIELD"
        result = self.generate(template="{{ city }}  {{ state }}", catalog_data=data)
        self.assertEqual(result["request_count"], 4)
        self.assertEqual(result["requests"][-1], "Springfield  IL")
        self.assertEqual(result["request_metadata"][-1]["geoids"], ["1772000", "1772001"])

    def test_template_replacement_is_not_recursive(self):
        data = self.catalog()
        data["places"][0]["city"] = "Literal {{state}}"
        result = self.generate(catalog_data=data)
        self.assertIn("Plumber Literal {{state}}, IL", result["requests"])

    def test_download_failure_has_no_partial_cache_and_no_retry(self):
        for error in (URLError("timeout"), TimeoutError("timeout"),
                      HTTPError("https://www2.census.gov/file", 404, "missing", {}, None)):
            with self.subTest(error=error):
                self.open.reset_mock()
                self.open.side_effect = error
                with self.assertRaises(geo.GazetteerError):
                    self.catalog()
                self.open.assert_called_once()
                self.assertFalse(self.config.cache_dir.exists())

    def test_second_download_failure_does_not_publish_half_snapshot(self):
        self.open.side_effect = [Response(archive("state", self.states)), URLError("unavailable")]
        with self.assertRaises(geo.GazetteerError):
            self.catalog()
        self.assertFalse(self.config.cache_dir.exists())

    def test_interrupted_chunked_response_has_consistent_error(self):
        response = Response(b"", length=False)
        response.read1 = Mock(side_effect=IncompleteRead(b"partial", 10))
        self.open.side_effect = lambda *a, **k: response
        with self.assertRaisesRegex(geo.GazetteerError, "download failed"):
            self.catalog()
        self.assertFalse(self.config.cache_dir.exists())

    def test_download_size_limits_with_and_without_content_length(self):
        for length in (True, False):
            with self.subTest(length=length):
                self.open.side_effect = lambda *a, **k: Response(b"x" * 33, length=length)
                with self.assertRaisesRegex(geo.GazetteerError, "size limit"):
                    geo.catalog("IL", config=replace(self.config, max_download_bytes=32))
                self.assertFalse(self.config.cache_dir.exists())

    def test_download_detects_truncation_and_deadline(self):
        response = Response(b"short")
        response.headers["Content-Length"] = "10"
        self.open.side_effect = lambda *a, **k: response
        with self.assertRaisesRegex(geo.GazetteerError, "truncated"):
            self.catalog()
        self.open.side_effect = lambda *a, **k: Response(b"first chunk", length=False)
        with patch.object(geo.time, "monotonic", side_effect=[0, 0.1, 2]):
            with self.assertRaisesRegex(geo.GazetteerError, "time limit"):
                geo.catalog("IL", config=replace(self.config, timeout_seconds=1))

    def test_redirects_are_not_followed(self):
        self.assertIsNone(geo._NoRedirect().redirect_request(None, None, 302, "", {}, "https://other.invalid"))

    def test_archive_member_uncompressed_size_and_crc_are_checked(self):
        cases = [archive("state", self.states, filename="../../outside.txt"), b"not a zip",
                 archive("state", self.states).replace(b"Alabama", b"Alabamx")]
        for data in cases:
            with self.subTest(data=data[:20]), self.assertRaises(geo.GazetteerError):
                geo._parse_archive(data, "state", self.config)
        with self.assertRaisesRegex(geo.GazetteerError, "uncompressed size"):
            geo._parse_archive(archive("state", self.states), "state", replace(self.config, max_uncompressed_bytes=20))
        extra = io.BytesIO(archive("state", self.states))
        with zipfile.ZipFile(extra, "a") as zipped:
            zipped.writestr("extra.txt", "unexpected")
        with self.assertRaisesRegex(geo.GazetteerError, "exactly"):
            geo._parse_archive(extra.getvalue(), "state", self.config)

    def test_delimited_schema_encoding_and_duplicate_geoids_are_checked(self):
        cases = ["USPS|NAME\nIL|Illinois\n", "USPS|GEOID|NAME|NAME\nIL|17|Illinois|Illinois\n",
                 "USPS|GEOID|NAME\nIL|17\n", "USPS|GEOID|NAME\nIL|17|Illinois|extra\n",
                 "USPS|GEOID|NAME\nIL|17|Illinois\nIL|17|Illinois\n",
                 "USPS|GEOID|NAME\n", "USPS|GEOID|NAME\nIL|1|Illinois\n",
                 b"USPS|GEOID|NAME\nIL|17|\xff\n"]
        for text in cases:
            with self.subTest(text=text), self.assertRaises(geo.GazetteerError):
                geo._parse_archive(archive("state", text=text), "state", self.config)

    def test_corrupt_deflate_stream_has_consistent_error(self):
        with patch.object(zipfile.ZipExtFile, "read", side_effect=zlib.error("invalid compressed data")):
            with self.assertRaisesRegex(geo.GazetteerError, "archive or delimited text is invalid"):
                geo._parse_archive(archive("state", self.states), "state", self.config)

    def test_inconsistent_state_lsad_or_name_cannot_create_partial_catalog(self):
        for change in ({"GEOID": "0100113"}, {"LSAD": "ZZ"}, {"NAME": "Wrong suffix village"}):
            with self.subTest(change=change):
                places = deepcopy(self.places)
                places[1].update(change)
                with self.assertRaises(geo.GazetteerError):
                    geo._assemble_dataset(self.states, places)
        with self.assertRaisesRegex(geo.GazetteerError, "missing places"):
            geo._assemble_dataset(self.states, [p for p in self.places if p["USPS"] != "AL"])

    def test_cache_integrity_errors_do_not_silently_refetch(self):
        self.catalog()
        path = geo._cache_path(self.config)
        self.open.reset_mock()
        envelope = json.loads(path.read_bytes())
        envelope["payload"]["places"][0]["city"] = "Tampered"
        path.write_text(json.dumps(envelope))
        with self.assertRaisesRegex(geo.GazetteerError, "corrupt"):
            self.catalog()
        self.open.assert_not_called()

    def test_cache_write_failure_cleans_temporary_file(self):
        with patch.object(geo.os, "link", side_effect=OSError("full disk")):
            with self.assertRaisesRegex(geo.GazetteerError, "cache"):
                self.catalog()
        self.assertEqual(list(self.config.cache_dir.iterdir()), [])

    def test_concurrent_cache_publication_keeps_existing_winner(self):
        self.catalog()
        path = geo._cache_path(self.config)
        winner = path.read_bytes()
        original = geo._read_cache
        self.places[1]["NAME"] = "Losing snapshot city"
        with patch.object(geo, "_read_cache", side_effect=[None, original(path, self.config)]):
            result = self.catalog()
        self.assertEqual(path.read_bytes(), winner)
        self.assertIn("Abingdon", [p["city"] for p in result["places"]])
        self.assertEqual(len(list(path.parent.iterdir())), 1)

    def test_cache_root_can_be_configured_by_environment(self):
        root = Path(self.temp.name) / "environment-cache"
        with patch.dict("os.environ", {"GEOGRAPHIC_REQUESTS_CACHE_DIR": str(root)}):
            geo.catalog("IL")
        self.assertTrue((root / "gazetteer-2025-v1.json").is_file())

    def test_invalid_configuration_is_rejected(self):
        for kwargs in ({"year": "2025"}, {"year": True}, {"year": 2000},
                       {"timeout_seconds": 0}, {"timeout_seconds": 61}, {"timeout_seconds": float("nan")},
                       {"max_download_bytes": 0}, {"max_uncompressed_bytes": 65 * 1024 * 1024}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                geo.DatasetConfig(**kwargs)

    def test_alias_and_import_have_no_network_or_cache_side_effects(self):
        self.assertIs(geo.generate, geo.generate_city_requests)
        spec = importlib.util.spec_from_file_location("geographic_requests_import_test", geo.__file__)
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {spec.name: module}), patch.object(
                Path, "mkdir", side_effect=AssertionError("Import must not create cache")), patch(
                "urllib.request.build_opener", side_effect=AssertionError("Import must not fetch")):
            spec.loader.exec_module(module)
        self.open.assert_not_called()


if __name__ == "__main__":
    unittest.main()
