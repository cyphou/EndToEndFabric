"""Tests for tmdl_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.tmdl_generator import generate_semantic_model


class TestTMDLGenerator(unittest.TestCase):
    """Test TMDL semantic model generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.sm_config = load_config_file("horizon-books", "semantic_model")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_tmdl_"))
        cls.result = generate_semantic_model(
            cls.industry_config, cls.sm_config, cls.tmpdir
        )

    def test_model_file_exists(self):
        model_files = self.result["model"]
        self.assertTrue(len(model_files) >= 1)
        model_tmdl = [p for p in model_files if p.name == "model.tmdl"]
        self.assertEqual(len(model_tmdl), 1)
        self.assertTrue(model_tmdl[0].is_file())

    def test_table_count(self):
        """Should generate 18 table .tmdl files (10 dims + 8 facts)."""
        tables = self.result["tables"]
        self.assertEqual(len(tables), 18)

    def test_relationship_count(self):
        """Should generate 14 relationship files."""
        rels = self.result["relationships"]
        self.assertEqual(len(rels), 14)

    def test_table_files_exist(self):
        for path in self.result["tables"]:
            self.assertTrue(path.is_file(), f"Missing: {path}")

    def test_relationship_files_exist(self):
        for path in self.result["relationships"]:
            self.assertTrue(path.is_file(), f"Missing: {path}")

    def test_tmdl_contains_columns(self):
        """Each table TMDL should contain column definitions."""
        for path in self.result["tables"]:
            content = path.read_text(encoding="utf-8")
            self.assertIn("column ", content, f"No columns in {path.name}")

    def test_tmdl_contains_partition(self):
        """Each table TMDL should contain a partition definition."""
        for path in self.result["tables"]:
            content = path.read_text(encoding="utf-8")
            self.assertIn("partition ", content, f"No partition in {path.name}")

    def test_measures_present(self):
        """Tables with assigned measures should contain measure definitions."""
        # FactFinancialTransactions gets finance measures
        fin_path = [p for p in self.result["tables"]
                     if p.name == "FactFinancialTransactions.tmdl"][0]
        content = fin_path.read_text(encoding="utf-8")
        self.assertIn("measure Total Revenue", content)
        self.assertIn("measure Gross Profit", content)

    def test_pbism_file(self):
        pbism = [p for p in self.result["model"] if p.name == "definition.pbism"]
        self.assertEqual(len(pbism), 1)
        self.assertTrue(pbism[0].is_file())

    def test_lineage_tags_deterministic(self):
        """Same input should produce same lineage tags."""
        tmpdir2 = Path(tempfile.mkdtemp(prefix="fabric_tmdl_2_"))
        result2 = generate_semantic_model(
            self.industry_config, self.sm_config, tmpdir2
        )
        for p1, p2 in zip(self.result["tables"], result2["tables"]):
            c1 = p1.read_text(encoding="utf-8")
            c2 = p2.read_text(encoding="utf-8")
            self.assertEqual(c1, c2, f"Non-deterministic: {p1.name}")


class TestTMDLMeasures(unittest.TestCase):
    """Test DAX measure generation."""

    @classmethod
    def setUpClass(cls):
        cls.sm_config = load_config_file("horizon-books", "semantic_model")

    def test_measure_count(self):
        measures = self.sm_config["semanticModel"]["measures"]
        self.assertEqual(len(measures), 20)

    def test_all_measures_have_expression(self):
        for m in self.sm_config["semanticModel"]["measures"]:
            self.assertTrue(m.get("expression"), f"No expr for {m['name']}")

    def test_all_measures_have_format(self):
        for m in self.sm_config["semanticModel"]["measures"]:
            self.assertIn("formatString", m, f"No format for {m['name']}")


if __name__ == "__main__":
    unittest.main()
