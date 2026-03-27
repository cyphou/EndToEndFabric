"""Tests for config_loader module."""

import unittest
from pathlib import Path

from core.config_loader import (
    list_industries,
    load_industry_config,
    load_config_file,
    load_all_configs,
    get_output_dir,
    IndustryNotFoundError,
    ConfigValidationError,
    _validate_against_schema,
    PROJECT_ROOT,
)


class TestListIndustries(unittest.TestCase):
    """Test industry discovery."""

    def test_returns_list(self):
        result = list_industries()
        self.assertIsInstance(result, list)

    def test_horizon_books_present(self):
        result = list_industries()
        self.assertIn("horizon-books", result)

    def test_sorted(self):
        result = list_industries()
        self.assertEqual(result, sorted(result))


class TestLoadIndustryConfig(unittest.TestCase):
    """Test loading and validating industry.json."""

    def test_load_horizon_books(self):
        config = load_industry_config("horizon-books")
        self.assertIn("industry", config)
        self.assertIn("fabricArtifacts", config)

    def test_industry_fields(self):
        config = load_industry_config("horizon-books")
        industry = config["industry"]
        self.assertEqual(industry["id"], "horizon-books")
        self.assertEqual(industry["name"], "Horizon Books")
        self.assertIn("theme", industry)
        self.assertIn("domains", industry)

    def test_fabric_artifacts(self):
        config = load_industry_config("horizon-books")
        artifacts = config["fabricArtifacts"]
        self.assertIn("lakehouses", artifacts)
        self.assertEqual(artifacts["lakehouses"]["bronze"], "BronzeLH")
        self.assertEqual(artifacts["lakehouses"]["silver"], "SilverLH")
        self.assertEqual(artifacts["lakehouses"]["gold"], "GoldLH")

    def test_not_found(self):
        with self.assertRaises(IndustryNotFoundError):
            load_industry_config("nonexistent-industry")

    def test_theme_colors(self):
        config = load_industry_config("horizon-books")
        theme = config["industry"]["theme"]
        self.assertTrue(theme["primary"].startswith("#"))
        self.assertTrue(theme["secondary"].startswith("#"))


class TestLoadConfigFile(unittest.TestCase):
    """Test loading specific config files."""

    def test_load_sample_data(self):
        config = load_config_file("horizon-books", "sample_data")
        self.assertIsNotNone(config)
        self.assertIn("sampleData", config)

    def test_load_semantic_model(self):
        config = load_config_file("horizon-books", "semantic_model")
        self.assertIsNotNone(config)
        self.assertIn("semanticModel", config)

    def test_load_reports(self):
        config = load_config_file("horizon-books", "reports")
        self.assertIsNotNone(config)
        self.assertIn("reports", config)

    def test_missing_config_returns_none(self):
        result = load_config_file("nonexistent-industry", "sample_data")
        self.assertIsNone(result)

    def test_invalid_key(self):
        with self.assertRaises(ValueError):
            load_config_file("horizon-books", "invalid_key")


class TestLoadAllConfigs(unittest.TestCase):
    """Test loading all configs for an industry."""

    def test_load_all(self):
        configs = load_all_configs("horizon-books")
        self.assertIn("industry", configs)
        self.assertIn("sample_data", configs)
        self.assertIn("semantic_model", configs)
        self.assertIn("reports", configs)

    def test_industry_always_present(self):
        configs = load_all_configs("horizon-books")
        self.assertIsNotNone(configs["industry"])


class TestSchemaValidation(unittest.TestCase):
    """Test JSON schema validation."""

    def test_valid_industry(self):
        config = load_industry_config("horizon-books")
        errors = _validate_against_schema(config, "industry")
        self.assertEqual(errors, [])

    def test_invalid_industry_missing_fields(self):
        invalid = {"industry": {"id": "test"}}
        errors = _validate_against_schema(invalid, "industry")
        self.assertTrue(len(errors) > 0)

    def test_invalid_theme_color(self):
        invalid = {
            "industry": {
                "id": "test",
                "name": "Test",
                "displayName": "Test Demo",
                "description": "Test",
                "domains": ["A", "B"],
                "dataYears": ["FY2024"],
                "theme": {"primary": "not-a-color", "secondary": "#000000"}
            },
            "fabricArtifacts": {
                "workspacePrefix": "Test",
                "lakehouses": {"bronze": "B", "silver": "S", "gold": "G"},
                "schemas": {"silver": ["s1"], "gold": ["g1"]}
            }
        }
        errors = _validate_against_schema(invalid, "industry")
        self.assertTrue(any("pattern" in e for e in errors))


class TestOutputDir(unittest.TestCase):
    """Test output directory resolution."""

    def test_default_output(self):
        result = get_output_dir("horizon-books")
        self.assertEqual(result, PROJECT_ROOT / "output" / "horizon-books")

    def test_custom_base(self):
        result = get_output_dir("test", Path("/tmp/custom"))
        self.assertEqual(result, Path("/tmp/custom/test"))


if __name__ == "__main__":
    unittest.main()
