"""Tests for cross-industry comparison generator."""

import shutil
import tempfile
from pathlib import Path
from unittest import TestCase

from core.comparison_generator import generate_comparison, _collect_metrics
from core.config_loader import list_industries, load_all_configs


class TestComparisonGenerator(TestCase):
    """Tests for comparison report generation."""

    def test_generate_comparison_produces_markdown(self):
        """Comparison report is a valid Markdown file."""
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            path = generate_comparison(out)
            self.assertTrue(path.exists())
            self.assertEqual(path.suffix, ".md")
            content = path.read_text(encoding="utf-8")
            self.assertIn("# Cross-Industry Comparison Report", content)

    def test_comparison_covers_all_industries(self):
        """Report mentions every configured industry."""
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            path = generate_comparison(out)
            content = path.read_text(encoding="utf-8")
            for ind_id in list_industries():
                configs = load_all_configs(ind_id)
                label = configs["industry"]["industry"]["name"]
                self.assertIn(label, content, f"Missing industry: {label}")

    def test_comparison_contains_all_sections(self):
        """Report has all expected section headers."""
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            path = generate_comparison(out)
            content = path.read_text(encoding="utf-8")
            for section in [
                "Data Volume",
                "Semantic Model Complexity",
                "Report Coverage",
                "Feature Flags",
                "HTAP Detail",
                "Forecast Detail",
            ]:
                self.assertIn(f"## {section}", content)

    def test_collect_metrics_returns_expected_keys(self):
        """_collect_metrics returns dict with all required keys."""
        ind_id = list_industries()[0]
        configs = load_all_configs(ind_id)
        metrics = _collect_metrics(ind_id, configs)
        expected_keys = {
            "id", "label", "csv_count", "csv_rows", "domain_count",
            "tables", "measures", "relationships", "calc_columns",
            "report_count", "total_pages", "analytics_pages",
            "forecast_pages", "htap_pages",
            "has_forecast", "has_htap", "has_writeback",
            "has_agent", "has_web_enrichment",
            "htap_streams", "htap_kql_tables",
            "forecast_models", "planning_models",
        }
        self.assertEqual(set(metrics.keys()), expected_keys)

    def test_metrics_values_are_non_negative(self):
        """All numeric metrics are >= 0."""
        for ind_id in list_industries():
            configs = load_all_configs(ind_id)
            metrics = _collect_metrics(ind_id, configs)
            for key, val in metrics.items():
                if isinstance(val, int):
                    self.assertGreaterEqual(val, 0, f"{ind_id}.{key} is negative")
