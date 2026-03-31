"""Tests for test_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_all_configs
from core.test_generator import generate_tests


class TestTestGenerator(unittest.TestCase):
    """Test the test_generator (Pester + validation script) output."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir = Path(tempfile.mkdtemp())
        cls.configs = load_all_configs("contoso-energy")
        cls.files = generate_tests(
            cls.configs["industry"], cls.configs, cls.tmpdir
        )

    def test_files_generated(self):
        self.assertGreaterEqual(len(self.files), 2)

    def test_pester_suite_exists(self):
        pester = self.tmpdir / "tests" / "ContosoEnergy.Tests.ps1"
        self.assertTrue(pester.exists())

    def test_validation_script_exists(self):
        val = self.tmpdir / "tests" / "Validate-ContosoEnergy.ps1"
        self.assertTrue(val.exists())

    def test_validation_contains_checks(self):
        val = (self.tmpdir / "tests" / "Validate-ContosoEnergy.ps1").read_text(encoding="utf-8")
        self.assertIn("CSV", val)
        self.assertIn("TMDL", val)
        self.assertIn("notebooks", val)

    def test_runner_exists(self):
        runner = self.tmpdir / "tests" / "Run-Tests.ps1"
        self.assertTrue(runner.exists())


if __name__ == "__main__":
    unittest.main()
