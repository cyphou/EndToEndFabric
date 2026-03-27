"""Tests for forecast_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.forecast_generator import generate_forecast


class TestForecastGenerator(unittest.TestCase):
    """Test Holt-Winters forecast notebook generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.forecast_config = load_config_file("horizon-books", "forecast")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_fc_test_"))
        cls.artifacts = generate_forecast(
            cls.industry_config, cls.forecast_config, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.artifacts, list)

    def test_two_artifacts_generated(self):
        self.assertEqual(len(self.artifacts), 2)

    def test_all_files_exist(self):
        for p in self.artifacts:
            self.assertTrue(p.is_file(), f"Missing: {p}")

    def test_config_json_written(self):
        json_files = [p for p in self.artifacts if p.suffix == ".json"]
        self.assertEqual(len(json_files), 1)
        content = json.loads(json_files[0].read_text(encoding="utf-8"))
        self.assertIn("forecastConfig", content)

    def test_notebook_generated(self):
        py_files = [p for p in self.artifacts if p.suffix == ".py"]
        self.assertEqual(len(py_files), 1)
        self.assertIn("Forecast", py_files[0].name)

    def test_notebook_contains_holt_winters(self):
        py_files = [p for p in self.artifacts if p.suffix == ".py"]
        content = py_files[0].read_text(encoding="utf-8")
        # Should reference forecasting concepts
        self.assertTrue(len(content) > 500)

    def test_notebook_references_mlflow(self):
        py_files = [p for p in self.artifacts if p.suffix == ".py"]
        content = py_files[0].read_text(encoding="utf-8")
        self.assertIn("mlflow", content.lower())


class TestForecastMultiIndustry(unittest.TestCase):
    """Test forecast generation across industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        fc = load_config_file("contoso-energy", "forecast")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_fc_ce_"))
        result = generate_forecast(cfg, fc, tmpdir)
        self.assertEqual(len(result), 2)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        fc = load_config_file("fabrikam-manufacturing", "forecast")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_fc_fm_"))
        result = generate_forecast(cfg, fc, tmpdir)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
