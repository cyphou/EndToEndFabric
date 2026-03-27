"""Tests for pester_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.pester_generator import generate_pester_tests


class TestPesterGenerator(unittest.TestCase):
    """Test Pester 5 test suite generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.sample_data_config = load_config_file("horizon-books", "sample_data")
        cls.semantic_model_config = load_config_file("horizon-books", "semantic_model")
        cls.reports_config = load_config_file("horizon-books", "reports")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_pester_test_"))
        cls.artifacts = generate_pester_tests(
            cls.industry_config,
            cls.sample_data_config,
            cls.semantic_model_config,
            cls.reports_config,
            cls.tmpdir,
        )

    def test_returns_list(self):
        self.assertIsInstance(self.artifacts, list)

    def test_two_artifacts_generated(self):
        self.assertEqual(len(self.artifacts), 2, "Expected .Tests.ps1 + Run-Tests.ps1")

    def test_all_files_exist(self):
        for p in self.artifacts:
            self.assertTrue(p.is_file(), f"Missing: {p}")

    def test_main_test_file_powershell(self):
        test_files = [p for p in self.artifacts if p.name.endswith(".Tests.ps1")]
        self.assertEqual(len(test_files), 1)

    def test_main_test_contains_describe(self):
        test_file = [p for p in self.artifacts if p.name.endswith(".Tests.ps1")][0]
        content = test_file.read_text(encoding="utf-8")
        self.assertIn("Describe", content)

    def test_main_test_contains_it_blocks(self):
        test_file = [p for p in self.artifacts if p.name.endswith(".Tests.ps1")][0]
        content = test_file.read_text(encoding="utf-8")
        self.assertIn("It ", content)

    def test_runner_script_present(self):
        runners = [p for p in self.artifacts if p.name == "Run-Tests.ps1"]
        self.assertEqual(len(runners), 1)

    def test_runner_invokes_pester(self):
        runner = [p for p in self.artifacts if p.name == "Run-Tests.ps1"][0]
        content = runner.read_text(encoding="utf-8")
        self.assertIn("Invoke-Pester", content)

    def test_csv_checks_present(self):
        test_file = [p for p in self.artifacts if p.name.endswith(".Tests.ps1")][0]
        content = test_file.read_text(encoding="utf-8")
        self.assertIn(".csv", content.lower())


class TestPesterMultiIndustry(unittest.TestCase):
    """Test pester generation across industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        sd = load_config_file("contoso-energy", "sample_data")
        sm = load_config_file("contoso-energy", "semantic_model")
        rp = load_config_file("contoso-energy", "reports")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_pester_ce_"))
        result = generate_pester_tests(cfg, sd, sm, rp, tmpdir)
        self.assertEqual(len(result), 2)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        sd = load_config_file("fabrikam-manufacturing", "sample_data")
        sm = load_config_file("fabrikam-manufacturing", "semantic_model")
        rp = load_config_file("fabrikam-manufacturing", "reports")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_pester_fm_"))
        result = generate_pester_tests(cfg, sd, sm, rp, tmpdir)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
