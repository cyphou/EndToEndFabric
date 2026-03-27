"""Tests for deploy_generator module."""

import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.deploy_generator import generate_deploy_scripts


class TestDeployGenerator(unittest.TestCase):
    """Test PowerShell deployment script generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.sample_data = load_config_file("horizon-books", "sample_data")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_deploy_test_"))
        cls.scripts = generate_deploy_scripts(
            cls.industry_config, cls.sample_data, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.scripts, list)

    def test_four_scripts_generated(self):
        self.assertEqual(len(self.scripts), 4)

    def test_all_files_exist(self):
        for s in self.scripts:
            self.assertTrue(s.is_file(), f"Missing: {s}")

    def test_deploy_full_exists(self):
        names = [p.name for p in self.scripts]
        self.assertIn("Deploy-Full.ps1", names)

    def test_upload_script_exists(self):
        names = [p.name for p in self.scripts]
        self.assertIn("Upload-SampleData.ps1", names)

    def test_validate_script_exists(self):
        names = [p.name for p in self.scripts]
        self.assertIn("Validate-Deployment.ps1", names)

    def test_psm1_module_exists(self):
        psm1_files = [p for p in self.scripts if p.suffix == ".psm1"]
        self.assertEqual(len(psm1_files), 1)

    def test_deploy_full_content(self):
        deploy_full = [p for p in self.scripts if p.name == "Deploy-Full.ps1"][0]
        content = deploy_full.read_text(encoding="utf-8")
        self.assertIn("WorkspaceId", content)
        self.assertTrue(len(content) > 200)

    def test_psm1_has_functions(self):
        psm1 = [p for p in self.scripts if p.suffix == ".psm1"][0]
        content = psm1.read_text(encoding="utf-8")
        self.assertIn("function", content.lower())

    def test_output_in_deploy_dir(self):
        for s in self.scripts:
            self.assertEqual(s.parent.name, "deploy")


class TestDeployMultiIndustry(unittest.TestCase):
    """Test deploy generation across industries."""

    def test_contoso_generates(self):
        cfg = load_industry_config("contoso-energy")
        sd = load_config_file("contoso-energy", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_deploy_ce_"))
        result = generate_deploy_scripts(cfg, sd, tmpdir)
        self.assertEqual(len(result), 4)

    def test_fabrikam_generates(self):
        cfg = load_industry_config("fabrikam-manufacturing")
        sd = load_config_file("fabrikam-manufacturing", "sample_data")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_deploy_fm_"))
        result = generate_deploy_scripts(cfg, sd, tmpdir)
        self.assertEqual(len(result), 4)
        psm1 = [p for p in result if p.suffix == ".psm1"]
        self.assertIn("FabrikamManufacturing", psm1[0].name)


if __name__ == "__main__":
    unittest.main()
