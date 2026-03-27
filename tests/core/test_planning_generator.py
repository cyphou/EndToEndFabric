"""Tests for planning_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.planning_generator import generate_planning


class TestPlanningGenerator(unittest.TestCase):
    """Test Planning IQ artifacts generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.planning_config = load_config_file("horizon-books", "planning")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_plan_test_"))
        cls.artifacts = generate_planning(
            cls.industry_config, cls.planning_config, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.artifacts, list)

    def test_three_artifacts_generated(self):
        self.assertEqual(len(self.artifacts), 3,
                         "Expected planning-config.json, SQLSetup, Populate")

    def test_all_files_exist(self):
        for p in self.artifacts:
            self.assertTrue(p.is_file(), f"Missing: {p}")

    def test_planning_config_json_valid(self):
        cfg = [p for p in self.artifacts if p.name == "planning-config.json"]
        self.assertEqual(len(cfg), 1)
        data = json.loads(cfg[0].read_text(encoding="utf-8"))
        self.assertIsInstance(data, dict)

    def test_sql_setup_notebook(self):
        sql = [p for p in self.artifacts if "SQLSetup" in p.name]
        self.assertEqual(len(sql), 1)
        content = sql[0].read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE", content.upper())

    def test_populate_notebook(self):
        pop = [p for p in self.artifacts if "Populate" in p.name]
        self.assertEqual(len(pop), 1)
        content = pop[0].read_text(encoding="utf-8")
        self.assertIn("spark", content.lower())

    def test_populate_has_scenario_data(self):
        pop = [p for p in self.artifacts if "Populate" in p.name][0]
        content = pop.read_text(encoding="utf-8")
        # Should reference scenario types
        self.assertTrue(
            "Base" in content or "scenario" in content.lower(),
            "Populate notebook should reference scenarios"
        )


class TestPlanningMultiIndustry(unittest.TestCase):
    """Test planning generation across industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        pc = load_config_file("contoso-energy", "planning")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_plan_ce_"))
        result = generate_planning(cfg, pc, tmpdir)
        self.assertEqual(len(result), 3)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        pc = load_config_file("fabrikam-manufacturing", "planning")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_plan_fm_"))
        result = generate_planning(cfg, pc, tmpdir)
        self.assertEqual(len(result), 3)


if __name__ == "__main__":
    unittest.main()
