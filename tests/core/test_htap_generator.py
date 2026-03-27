"""Tests for htap_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.htap_generator import generate_htap


class TestHTAPGenerator(unittest.TestCase):
    """Test HTAP / Transactional Analytics generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.htap_config = load_config_file("horizon-books", "htap")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_htap_test_"))
        cls.artifacts = generate_htap(
            cls.industry_config, cls.htap_config, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.artifacts, list)

    def test_six_artifacts_generated(self):
        self.assertEqual(len(self.artifacts), 6)

    def test_all_files_exist(self):
        for p in self.artifacts:
            self.assertTrue(p.is_file(), f"Missing: {p}")

    def test_eventhouse_definition(self):
        eh_files = [p for p in self.artifacts if "eventhouse" in p.name.lower()]
        self.assertEqual(len(eh_files), 1)
        content = json.loads(eh_files[0].read_text(encoding="utf-8"))
        self.assertIsInstance(content, dict)

    def test_kql_database_script(self):
        kql_files = [p for p in self.artifacts
                     if p.suffix == ".kql" and "database" in p.name.lower()]
        self.assertEqual(len(kql_files), 1)
        content = kql_files[0].read_text(encoding="utf-8")
        self.assertIn(".create", content.lower())

    def test_event_simulator_notebook(self):
        py_files = [p for p in self.artifacts if p.suffix == ".py"]
        self.assertEqual(len(py_files), 1)
        content = py_files[0].read_text(encoding="utf-8")
        self.assertTrue(len(content) > 500)

    def test_bridge_queries(self):
        bridge = [p for p in self.artifacts if "bridge" in p.name.lower()]
        self.assertGreaterEqual(len(bridge), 1)

    def test_readme_generated(self):
        readmes = [p for p in self.artifacts if p.name == "README.md"]
        self.assertEqual(len(readmes), 1)

    def test_htap_config_written(self):
        cfg_files = [p for p in self.artifacts if "htap-config" in p.name]
        self.assertEqual(len(cfg_files), 1)


class TestHTAPMultiIndustry(unittest.TestCase):
    """Test HTAP generation across industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        hc = load_config_file("contoso-energy", "htap")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_htap_ce_"))
        result = generate_htap(cfg, hc, tmpdir)
        self.assertEqual(len(result), 6)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        hc = load_config_file("fabrikam-manufacturing", "htap")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_htap_fm_"))
        result = generate_htap(cfg, hc, tmpdir)
        self.assertEqual(len(result), 6)


if __name__ == "__main__":
    unittest.main()
