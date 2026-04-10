"""Tests for copilot_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.copilot_generator import generate_copilot_instructions


class TestCopilotGenerator(unittest.TestCase):
    """Test Copilot instructions generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.sm_config = load_config_file("contoso-energy", "semantic_model")
        cls.sd_config = load_config_file("contoso-energy", "sample_data")

    def test_generates_one_file(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        result = generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].name, "instructions.md")

    def test_file_in_copilot_dir(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        result = generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        self.assertEqual(result[0].parent.name, ".copilot")

    def test_content_has_company_name(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        content = (tmpdir / ".copilot" / "instructions.md").read_text()
        self.assertIn("Contoso", content)

    def test_content_has_semantic_model_section(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        content = (tmpdir / ".copilot" / "instructions.md").read_text()
        self.assertIn("## Semantic Model", content)

    def test_content_has_data_domains(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        content = (tmpdir / ".copilot" / "instructions.md").read_text()
        self.assertIn("## Data Domains", content)

    def test_content_has_theme(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        content = (tmpdir / ".copilot" / "instructions.md").read_text()
        self.assertIn("## Theme", content)

    def test_content_has_naming_conventions(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        generate_copilot_instructions(
            self.industry_config, self.sm_config, self.sd_config, tmpdir)
        content = (tmpdir / ".copilot" / "instructions.md").read_text()
        self.assertIn("## Naming Conventions", content)

    def test_works_without_semantic_model(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        result = generate_copilot_instructions(
            self.industry_config, None, self.sd_config, tmpdir)
        self.assertEqual(len(result), 1)

    def test_works_without_sample_data(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="copilot_"))
        result = generate_copilot_instructions(
            self.industry_config, self.sm_config, None, tmpdir)
        self.assertEqual(len(result), 1)

    def test_all_industries_generate(self):
        from core.config_loader import INDUSTRIES_DIR
        industries = sorted(
            d.name for d in Path(INDUSTRIES_DIR).iterdir()
            if d.is_dir() and (d / "industry.json").exists()
        )
        for ind in industries:
            with self.subTest(industry=ind):
                ic = load_industry_config(ind)
                sm = load_config_file(ind, "semantic_model")
                sd = load_config_file(ind, "sample_data")
                tmpdir = Path(tempfile.mkdtemp(prefix=f"copilot_{ind}_"))
                result = generate_copilot_instructions(ic, sm, sd, tmpdir)
                self.assertEqual(len(result), 1)
                self.assertTrue(result[0].exists())


if __name__ == "__main__":
    unittest.main()
