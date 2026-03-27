"""Industry config validation tests.

Validates every industry's JSON configuration files against the expected
schema contracts — ensuring that all industries remain generable without
runtime errors.
"""

import json
import unittest
from pathlib import Path

from core.config_loader import (
    CONFIG_FILES,
    INDUSTRIES_DIR,
    load_industry_config,
    load_config_file,
)

# Auto-discover all industries that have an industry.json
INDUSTRY_IDS = sorted(
    d.name for d in Path(INDUSTRIES_DIR).iterdir()
    if d.is_dir() and (d / "industry.json").exists()
)


class TestAllIndustriesLoadable(unittest.TestCase):
    """Every industry's config files must load without error."""

    def test_all_industry_configs_load(self):
        for ind in INDUSTRY_IDS:
            with self.subTest(industry=ind):
                cfg = load_industry_config(ind)
                self.assertIn("industry", cfg)
                self.assertIn("name", cfg["industry"])

    def test_all_config_files_load(self):
        for ind in INDUSTRY_IDS:
            for key in CONFIG_FILES:
                if key == "industry":
                    continue
                with self.subTest(industry=ind, config=key):
                    # May return None if file doesn't exist
                    result = load_config_file(ind, key)
                    if result is not None:
                        self.assertIsInstance(result, dict)


class TestIndustrySchemaContracts(unittest.TestCase):
    """Validate key schema contracts across all industries."""

    def test_sample_data_has_domains(self):
        for ind in INDUSTRY_IDS:
            sd = load_config_file(ind, "sample_data")
            if sd is None:
                continue
            with self.subTest(industry=ind):
                domains = sd.get("sampleData", {}).get("domains", [])
                self.assertGreater(len(domains), 0, f"{ind} has no domains")
                for d in domains:
                    self.assertIn("name", d)
                    self.assertIn("tables", d)
                    self.assertGreater(len(d["tables"]), 0)

    def test_semantic_model_has_tables(self):
        for ind in INDUSTRY_IDS:
            sm = load_config_file(ind, "semantic_model")
            if sm is None:
                continue
            with self.subTest(industry=ind):
                tables = sm.get("semanticModel", {}).get("tables", [])
                self.assertGreater(len(tables), 0)

    def test_semantic_model_has_measures(self):
        for ind in INDUSTRY_IDS:
            sm = load_config_file(ind, "semantic_model")
            if sm is None:
                continue
            with self.subTest(industry=ind):
                measures = sm.get("semanticModel", {}).get("measures", [])
                self.assertGreater(len(measures), 0)

    def test_semantic_model_has_relationships(self):
        for ind in INDUSTRY_IDS:
            sm = load_config_file(ind, "semantic_model")
            if sm is None:
                continue
            with self.subTest(industry=ind):
                rels = sm.get("semanticModel", {}).get("relationships", [])
                self.assertGreater(len(rels), 0)

    def test_reports_has_reports(self):
        for ind in INDUSTRY_IDS:
            rp = load_config_file(ind, "reports")
            if rp is None:
                continue
            with self.subTest(industry=ind):
                reports = rp.get("reports", [])
                self.assertGreater(len(reports), 0)
                for r in reports:
                    self.assertIn("name", r)
                    self.assertIn("pages", r)

    def test_forecast_has_models(self):
        for ind in INDUSTRY_IDS:
            fc = load_config_file(ind, "forecast")
            if fc is None:
                continue
            with self.subTest(industry=ind):
                fc_inner = fc.get("forecastConfig", fc)
                models = fc_inner.get("models", fc_inner.get("forecastModels", []))
                self.assertGreater(len(models), 0)

    def test_planning_has_models(self):
        for ind in INDUSTRY_IDS:
            pc = load_config_file(ind, "planning")
            if pc is None:
                continue
            with self.subTest(industry=ind):
                pc_inner = pc.get("planningConfig", pc)
                models = pc_inner.get("models", pc_inner.get("planningModels", []))
                self.assertGreater(len(models), 0)

    def test_htap_has_event_streams(self):
        for ind in INDUSTRY_IDS:
            hc = load_config_file(ind, "htap")
            if hc is None:
                continue
            with self.subTest(industry=ind):
                hc_inner = hc.get("htapConfig", hc)
                streams = hc_inner.get("eventStreams", [])
                self.assertGreater(len(streams), 0)


class TestIndustryArtifactNames(unittest.TestCase):
    """Validate fabricArtifacts references in each industry."""

    def test_lakehouses_defined(self):
        for ind in INDUSTRY_IDS:
            cfg = load_industry_config(ind)
            with self.subTest(industry=ind):
                lhs = cfg.get("fabricArtifacts", {}).get("lakehouses", {})
                self.assertIn("bronze", lhs)
                self.assertIn("silver", lhs)
                self.assertIn("gold", lhs)


if __name__ == "__main__":
    unittest.main()
