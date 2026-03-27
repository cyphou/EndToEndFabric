"""Tests for notebook_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.notebook_generator import generate_notebooks


class TestNotebookGenerator(unittest.TestCase):
    """Test PySpark notebook generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.sample_data = load_config_file("horizon-books", "sample_data")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_nb_test_"))
        cls.notebooks = generate_notebooks(
            cls.industry_config, cls.sample_data, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.notebooks, list)

    def test_notebooks_generated(self):
        self.assertGreaterEqual(len(self.notebooks), 4)

    def test_all_files_exist(self):
        for nb_path in self.notebooks:
            self.assertTrue(nb_path.is_file(), f"Missing: {nb_path}")

    def test_files_are_python(self):
        for nb_path in self.notebooks:
            self.assertEqual(nb_path.suffix, ".py")

    def test_nb01_exists(self):
        names = [p.name for p in self.notebooks]
        self.assertTrue(
            any("BronzeToSilver" in n or "Bronze_to_Silver" in n for n in names),
            f"NB01 not found in {names}",
        )

    def test_nb06_exists(self):
        names = [p.name for p in self.notebooks]
        self.assertTrue(
            any("Diagnostic" in n for n in names),
            f"NB06 not found in {names}",
        )

    def test_notebook_contains_spark_code(self):
        for nb_path in self.notebooks:
            content = nb_path.read_text(encoding="utf-8")
            self.assertTrue(len(content) > 100,
                            f"Notebook {nb_path.name} is too small")

    def test_notebook_references_lakehouse(self):
        nb01 = [p for p in self.notebooks if "Bronze" in p.name]
        if nb01:
            content = nb01[0].read_text(encoding="utf-8")
            self.assertIn("BronzeLH", content)

    def test_output_in_notebooks_dir(self):
        for nb_path in self.notebooks:
            self.assertEqual(nb_path.parent.name, "notebooks")


class TestNotebookGeneratorMultiIndustry(unittest.TestCase):
    """Test notebook generation across different industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        sd = load_config_file("contoso-energy", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_nb_ce_"))
        result = generate_notebooks(cfg, sd, tmpdir)
        self.assertGreaterEqual(len(result), 4)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        sd = load_config_file("fabrikam-manufacturing", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_nb_fm_"))
        result = generate_notebooks(cfg, sd, tmpdir)
        self.assertGreaterEqual(len(result), 4)


if __name__ == "__main__":
    unittest.main()
