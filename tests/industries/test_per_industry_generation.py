"""Per-industry generation tests — PLAN.md §10.3 test matrix.

Validates that each industry generates the expected artifact counts
matching the targets in PLAN.md.
"""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_all_configs, get_output_dir
from core.csv_generator import generate_all_csvs
from core.tmdl_generator import generate_semantic_model
from core.notebook_generator import generate_notebooks


# Expected counts from PLAN.md §10.3
EXPECTED = {
    "contoso-energy": {
        "csv_min": 20, "tmdl_tables": 28, "measures_min": 110,
        "relationships_min": 30, "notebooks": 4,
    },
    "horizon-books": {
        "csv_min": 17, "tmdl_tables": 23, "measures_min": 96,
        "relationships_min": 27, "notebooks": 4,
    },
    "northwind-hrfinance": {
        "csv_min": 22, "tmdl_tables": 30, "measures_min": 130,
        "relationships_min": 38, "notebooks": 4,
    },
    "fabrikam-manufacturing": {
        "csv_min": 25, "tmdl_tables": 32, "measures_min": 120,
        "relationships_min": 35, "notebooks": 4,
    },
}


class TestContosoEnergyGeneration(unittest.TestCase):
    """Contoso Energy demo generation meets PLAN.md targets."""

    industry_id = "contoso-energy"

    @classmethod
    def setUpClass(cls):
        cls.configs = load_all_configs(cls.industry_id)
        cls.tmpdir = Path(tempfile.mkdtemp(prefix=f"fabric_{cls.industry_id}_"))
        cls.exp = EXPECTED[cls.industry_id]

    def test_csv_count(self):
        paths = generate_all_csvs(self.configs["sample_data"], self.tmpdir, seed=42)
        self.assertGreaterEqual(len(paths), self.exp["csv_min"])

    def test_tmdl_tables(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertEqual(len(result["tables"]), self.exp["tmdl_tables"])

    def test_tmdl_relationships(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertGreaterEqual(len(result["relationships"]), self.exp["relationships_min"])

    def test_measure_count(self):
        measures = self.configs["semantic_model"]["semanticModel"]["measures"]
        self.assertGreaterEqual(len(measures), self.exp["measures_min"])

    def test_notebooks(self):
        paths = generate_notebooks(self.configs["industry"], self.configs.get("sample_data"), self.tmpdir)
        self.assertEqual(len(paths), self.exp["notebooks"])


class TestHorizonBooksGeneration(unittest.TestCase):
    """Horizon Books demo generation meets PLAN.md targets."""

    industry_id = "horizon-books"

    @classmethod
    def setUpClass(cls):
        cls.configs = load_all_configs(cls.industry_id)
        cls.tmpdir = Path(tempfile.mkdtemp(prefix=f"fabric_{cls.industry_id}_"))
        cls.exp = EXPECTED[cls.industry_id]

    def test_csv_count(self):
        paths = generate_all_csvs(self.configs["sample_data"], self.tmpdir, seed=42)
        self.assertGreaterEqual(len(paths), self.exp["csv_min"])

    def test_tmdl_tables(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertEqual(len(result["tables"]), self.exp["tmdl_tables"])

    def test_tmdl_relationships(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertGreaterEqual(len(result["relationships"]), self.exp["relationships_min"])

    def test_measure_count(self):
        measures = self.configs["semantic_model"]["semanticModel"]["measures"]
        self.assertGreaterEqual(len(measures), self.exp["measures_min"])

    def test_notebooks(self):
        paths = generate_notebooks(self.configs["industry"], self.configs.get("sample_data"), self.tmpdir)
        self.assertEqual(len(paths), self.exp["notebooks"])


class TestNorthwindGeneration(unittest.TestCase):
    """Northwind HR/Finance demo generation meets PLAN.md targets."""

    industry_id = "northwind-hrfinance"

    @classmethod
    def setUpClass(cls):
        cls.configs = load_all_configs(cls.industry_id)
        cls.tmpdir = Path(tempfile.mkdtemp(prefix=f"fabric_{cls.industry_id}_"))
        cls.exp = EXPECTED[cls.industry_id]

    def test_csv_count(self):
        paths = generate_all_csvs(self.configs["sample_data"], self.tmpdir, seed=42)
        self.assertGreaterEqual(len(paths), self.exp["csv_min"])

    def test_tmdl_tables(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertEqual(len(result["tables"]), self.exp["tmdl_tables"])

    def test_tmdl_relationships(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertGreaterEqual(len(result["relationships"]), self.exp["relationships_min"])

    def test_measure_count(self):
        measures = self.configs["semantic_model"]["semanticModel"]["measures"]
        self.assertGreaterEqual(len(measures), self.exp["measures_min"])

    def test_notebooks(self):
        paths = generate_notebooks(self.configs["industry"], self.configs.get("sample_data"), self.tmpdir)
        self.assertEqual(len(paths), self.exp["notebooks"])


class TestFabrikamGeneration(unittest.TestCase):
    """Fabrikam Manufacturing demo generation meets PLAN.md targets."""

    industry_id = "fabrikam-manufacturing"

    @classmethod
    def setUpClass(cls):
        cls.configs = load_all_configs(cls.industry_id)
        cls.tmpdir = Path(tempfile.mkdtemp(prefix=f"fabric_{cls.industry_id}_"))
        cls.exp = EXPECTED[cls.industry_id]

    def test_csv_count(self):
        paths = generate_all_csvs(self.configs["sample_data"], self.tmpdir, seed=42)
        self.assertGreaterEqual(len(paths), self.exp["csv_min"])

    def test_tmdl_tables(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertEqual(len(result["tables"]), self.exp["tmdl_tables"])

    def test_tmdl_relationships(self):
        result = generate_semantic_model(
            self.configs["industry"], self.configs["semantic_model"], self.tmpdir)
        self.assertGreaterEqual(len(result["relationships"]), self.exp["relationships_min"])

    def test_measure_count(self):
        measures = self.configs["semantic_model"]["semanticModel"]["measures"]
        self.assertGreaterEqual(len(measures), self.exp["measures_min"])

    def test_notebooks(self):
        paths = generate_notebooks(self.configs["industry"], self.configs.get("sample_data"), self.tmpdir)
        self.assertEqual(len(paths), self.exp["notebooks"])


if __name__ == "__main__":
    unittest.main()
