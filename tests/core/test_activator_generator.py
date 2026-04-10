"""Tests for activator_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.activator_generator import generate_data_activator


class TestActivatorGenerator(unittest.TestCase):
    """Test Data Activator Reflex trigger generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.htap_config = load_config_file("contoso-energy", "htap")

    def test_generates_two_files(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        result = generate_data_activator(
            self.industry_config, self.htap_config, tmpdir)
        self.assertEqual(len(result), 2)
        names = {p.name for p in result}
        self.assertIn("reflex-definition.json", names)
        self.assertIn("README.md", names)

    def test_files_in_data_activator_dir(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        result = generate_data_activator(
            self.industry_config, self.htap_config, tmpdir)
        for p in result:
            self.assertEqual(p.parent.name, "DataActivator")

    def test_reflex_json_valid(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        result = generate_data_activator(
            self.industry_config, self.htap_config, tmpdir)
        reflex_path = [p for p in result if p.name == "reflex-definition.json"][0]
        data = json.loads(reflex_path.read_text())
        self.assertIn("name", data)
        self.assertIn("triggers", data)
        self.assertIsInstance(data["triggers"], list)
        self.assertGreater(len(data["triggers"]), 0)

    def test_trigger_has_required_fields(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        generate_data_activator(
            self.industry_config, self.htap_config, tmpdir)
        data = json.loads((tmpdir / "DataActivator" / "reflex-definition.json").read_text())
        for trigger in data["triggers"]:
            self.assertIn("name", trigger)
            self.assertIn("source", trigger)
            self.assertIn("condition", trigger)
            self.assertIn("action", trigger)

    def test_metadata_present(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        generate_data_activator(
            self.industry_config, self.htap_config, tmpdir)
        data = json.loads((tmpdir / "DataActivator" / "reflex-definition.json").read_text())
        self.assertIn("metadata", data)
        self.assertEqual(data["metadata"]["industry"], "contoso-energy")

    def test_returns_empty_when_no_htap_config(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        result = generate_data_activator(self.industry_config, None, tmpdir)
        self.assertEqual(result, [])

    def test_readme_contains_trigger_table(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="activator_"))
        generate_data_activator(
            self.industry_config, self.htap_config, tmpdir)
        content = (tmpdir / "DataActivator" / "README.md").read_text()
        self.assertIn("## Triggers", content)
        self.assertIn("Source", content)

    def test_all_industries_generate(self):
        from core.config_loader import INDUSTRIES_DIR
        industries = sorted(
            d.name for d in Path(INDUSTRIES_DIR).iterdir()
            if d.is_dir() and (d / "industry.json").exists()
        )
        for ind in industries:
            with self.subTest(industry=ind):
                ic = load_industry_config(ind)
                htap = load_config_file(ind, "htap")
                tmpdir = Path(tempfile.mkdtemp(prefix=f"activator_{ind}_"))
                result = generate_data_activator(ic, htap, tmpdir)
                # All industries should have HTAP config, so should generate
                self.assertEqual(len(result), 2, f"{ind} should generate 2 files")


if __name__ == "__main__":
    unittest.main()
