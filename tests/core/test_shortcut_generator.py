"""Tests for shortcut_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.shortcut_generator import generate_shortcuts


class TestShortcutGenerator(unittest.TestCase):
    """Test Lakehouse shortcut generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.sd_config = load_config_file("contoso-energy", "sample_data")

    def test_generates_two_files(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="shortcut_"))
        result = generate_shortcuts(
            self.industry_config, self.sd_config, tmpdir)
        self.assertEqual(len(result), 2)
        names = {p.name for p in result}
        self.assertIn("shortcuts.json", names)
        self.assertIn("README.md", names)

    def test_files_in_shortcuts_dir(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="shortcut_"))
        result = generate_shortcuts(
            self.industry_config, self.sd_config, tmpdir)
        for p in result:
            self.assertEqual(p.parent.name, "Shortcuts")

    def test_json_valid(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="shortcut_"))
        generate_shortcuts(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Shortcuts" / "shortcuts.json").read_text())
        self.assertIn("name", data)
        self.assertIn("lakehouses", data)
        self.assertIn("BronzeLH", data["lakehouses"])

    def test_has_three_target_types(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="shortcut_"))
        generate_shortcuts(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Shortcuts" / "shortcuts.json").read_text())
        bronze = data["lakehouses"]["BronzeLH"]
        self.assertIn("oneLakeShortcuts", bronze)
        self.assertIn("adlsGen2Shortcuts", bronze)
        self.assertIn("s3Shortcuts", bronze)

    def test_shortcut_count_matches_tables(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="shortcut_"))
        generate_shortcuts(self.industry_config, self.sd_config, tmpdir)
        data = json.loads((tmpdir / "Shortcuts" / "shortcuts.json").read_text())
        count = data["metadata"]["totalShortcuts"]
        self.assertGreater(count, 0)
        self.assertEqual(
            len(data["lakehouses"]["BronzeLH"]["oneLakeShortcuts"]), count)

    def test_returns_empty_when_no_sample_data(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="shortcut_"))
        result = generate_shortcuts(self.industry_config, None, tmpdir)
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
                tmpdir = Path(tempfile.mkdtemp(prefix=f"shortcut_{ind}_"))
                result = generate_shortcuts(ic, sd, tmpdir)
                self.assertEqual(len(result), 2, f"{ind} should generate 2 files")


if __name__ == "__main__":
    unittest.main()
