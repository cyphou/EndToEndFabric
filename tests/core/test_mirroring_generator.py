"""Tests for mirroring_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.mirroring_generator import generate_mirroring


class TestMirroringGenerator(unittest.TestCase):
    """Test Fabric Mirroring definition generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.sd_config = load_config_file("contoso-energy", "sample_data")

    def test_generates_two_files(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        result = generate_mirroring(
            self.industry_config, self.sd_config, tmpdir)
        self.assertEqual(len(result), 2)
        names = {p.name for p in result}
        self.assertIn("mirroring-definition.json", names)
        self.assertIn("README.md", names)

    def test_files_in_mirroring_dir(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        result = generate_mirroring(
            self.industry_config, self.sd_config, tmpdir)
        for p in result:
            self.assertEqual(p.parent.name, "Mirroring")

    def test_json_valid(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        generate_mirroring(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Mirroring" / "mirroring-definition.json").read_text())
        self.assertIn("name", data)
        self.assertIn("sourceConfigurations", data)
        self.assertIn("destination", data)

    def test_has_five_source_types(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        generate_mirroring(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Mirroring" / "mirroring-definition.json").read_text())
        sources = data["sourceConfigurations"]
        self.assertEqual(len(sources), 5)
        self.assertIn("sqlServer", sources)
        self.assertIn("azureSql", sources)
        self.assertIn("cosmosDb", sources)
        self.assertIn("postgresql", sources)
        self.assertIn("snowflake", sources)

    def test_replication_policy(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        generate_mirroring(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Mirroring" / "mirroring-definition.json").read_text())
        policy = data["replicationPolicy"]
        self.assertEqual(policy["mode"], "continuous")
        self.assertTrue(policy["changeDataCapture"])

    def test_table_count_matches_metadata(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        generate_mirroring(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Mirroring" / "mirroring-definition.json").read_text())
        table_count = data["metadata"]["tableCount"]
        # Check that tables are listed in each source config
        for src_type, src_cfg in data["sourceConfigurations"].items():
            self.assertEqual(len(src_cfg["tables"]), table_count,
                             f"{src_type} has wrong table count")

    def test_returns_empty_when_no_sample_data(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="mirroring_"))
        result = generate_mirroring(self.industry_config, None, tmpdir)
        self.assertEqual(result, [])

    def test_all_industries_generate(self):
        from core.config_loader import INDUSTRIES_DIR
        industries = sorted(
            d.name for d in Path(INDUSTRIES_DIR).iterdir()
            if d.is_dir() and (d / "industry.json").exists()
        )
        for ind in industries:
            with self.subTest(industry=ind):
                ic = load_industry_config(ind)
                sd = load_config_file(ind, "sample_data")
                tmpdir = Path(tempfile.mkdtemp(prefix=f"mirroring_{ind}_"))
                result = generate_mirroring(ic, sd, tmpdir)
                self.assertEqual(len(result), 2, f"{ind} should generate 2 files")


if __name__ == "__main__":
    unittest.main()
