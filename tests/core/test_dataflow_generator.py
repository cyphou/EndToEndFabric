"""Tests for dataflow_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.dataflow_generator import generate_dataflows


class TestDataflowGenerator(unittest.TestCase):
    """Test Dataflow Gen2 generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.sample_data = load_config_file("horizon-books", "sample_data")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_df_test_"))
        cls.dataflows = generate_dataflows(
            cls.industry_config, cls.sample_data, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.dataflows, list)

    def test_dataflows_generated(self):
        self.assertGreaterEqual(len(self.dataflows), 3)

    def test_all_files_exist(self):
        for df_path in self.dataflows:
            self.assertTrue(df_path.is_file(), f"Missing: {df_path}")

    def test_json_files_valid(self):
        for df_path in self.dataflows:
            if df_path.suffix == ".json":
                content = df_path.read_text(encoding="utf-8")
                parsed = json.loads(content)
                self.assertIsInstance(parsed, dict)

    def test_one_dataflow_per_domain(self):
        domains = self.sample_data["sampleData"]["domains"]
        json_files = [p for p in self.dataflows if p.suffix == ".json"]
        self.assertEqual(len(json_files), len(domains))

    def test_dataflow_contains_queries(self):
        json_files = [p for p in self.dataflows if p.suffix == ".json"]
        for df_path in json_files:
            content = json.loads(df_path.read_text(encoding="utf-8"))
            dataflow = content.get("dataflow", {})
            queries = dataflow.get("queries", [])
            self.assertGreater(len(queries), 0,
                               f"No queries in {df_path.name}")

    def test_dataflow_references_bronze_lh(self):
        json_files = [p for p in self.dataflows if p.suffix == ".json"]
        for df_path in json_files:
            content = json.loads(df_path.read_text(encoding="utf-8"))
            dataflow = content.get("dataflow", {})
            self.assertIn("BronzeLH", dataflow.get("destinationLakehouse", ""))

    def test_output_in_dataflows_dir(self):
        for df_path in self.dataflows:
            self.assertEqual(df_path.parent.name, "Dataflows")


class TestDataflowMultiIndustry(unittest.TestCase):
    """Test dataflow generation across industries."""

    def test_contoso_domains(self):
        cfg = load_industry_config("contoso-energy")
        sd = load_config_file("contoso-energy", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_df_ce_"))
        result = generate_dataflows(cfg, sd, tmpdir)
        json_files = [p for p in result if p.suffix == ".json"]
        self.assertGreaterEqual(len(json_files), 5)

    def test_fabrikam_domains(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        sd = load_config_file("fabrikam-manufacturing", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_df_fm_"))
        result = generate_dataflows(cfg, sd, tmpdir)
        json_files = [p for p in result if p.suffix == ".json"]
        self.assertGreaterEqual(len(json_files), 5)


if __name__ == "__main__":
    unittest.main()
