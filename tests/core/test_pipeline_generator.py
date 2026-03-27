"""Tests for pipeline_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.pipeline_generator import generate_pipeline


class TestPipelineGenerator(unittest.TestCase):
    """Test Data Pipeline generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.sample_data_config = load_config_file("horizon-books", "sample_data")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_pipe_test_"))
        cls.artifacts = generate_pipeline(
            cls.industry_config, cls.sample_data_config, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.artifacts, list)

    def test_two_artifacts_generated(self):
        self.assertEqual(len(self.artifacts), 2, "Expected pipeline-content.json + README")

    def test_all_files_exist(self):
        for p in self.artifacts:
            self.assertTrue(p.is_file(), f"Missing: {p}")

    def test_pipeline_content_json_valid(self):
        pc = [p for p in self.artifacts if p.name == "pipeline-content.json"]
        self.assertEqual(len(pc), 1)
        data = json.loads(pc[0].read_text(encoding="utf-8"))
        self.assertIn("properties", data)
        self.assertIn("activities", data["properties"])

    def test_pipeline_has_activities(self):
        pc = [p for p in self.artifacts if p.name == "pipeline-content.json"][0]
        data = json.loads(pc.read_text(encoding="utf-8"))
        activities = data.get("properties", {}).get("activities", [])
        self.assertGreater(len(activities), 0)

    def test_pipeline_references_dataflows(self):
        pc = [p for p in self.artifacts if p.name == "pipeline-content.json"][0]
        content = pc.read_text(encoding="utf-8")
        self.assertIn("RefreshDataflow", content)

    def test_pipeline_references_notebooks(self):
        pc = [p for p in self.artifacts if p.name == "pipeline-content.json"][0]
        content = pc.read_text(encoding="utf-8")
        self.assertIn("Notebook", content)

    def test_readme_generated(self):
        readmes = [p for p in self.artifacts if p.name == "README.md"]
        self.assertEqual(len(readmes), 1)


class TestPipelineMultiIndustry(unittest.TestCase):
    """Test pipeline generation across multiple industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        sd = load_config_file("contoso-energy", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_pipe_ce_"))
        result = generate_pipeline(cfg, sd, tmpdir)
        self.assertEqual(len(result), 2)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        sd = load_config_file("fabrikam-manufacturing", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_pipe_fm_"))
        result = generate_pipeline(cfg, sd, tmpdir)
        self.assertEqual(len(result), 2)


if __name__ == "__main__":
    unittest.main()
