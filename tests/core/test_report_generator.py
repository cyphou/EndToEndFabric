"""Tests for report_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.report_generator import generate_reports


class TestReportGenerator(unittest.TestCase):
    """Test PBIR report generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("horizon-books")
        cls.reports_config = load_config_file("horizon-books", "reports")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_report_"))
        cls.result = generate_reports(
            cls.industry_config, cls.reports_config, cls.tmpdir
        )

    def test_reports_generated(self):
        self.assertTrue(len(self.result) > 0)

    def test_analytics_report_dir(self):
        report_dir = self.tmpdir / "HorizonBooksAnalytics.Report"
        self.assertTrue(report_dir.is_dir())

    def test_forecasting_report_dir(self):
        report_dir = self.tmpdir / "HorizonBooksForecasting.Report"
        self.assertTrue(report_dir.is_dir())

    def test_report_json_exists(self):
        rpt_json = self.tmpdir / "HorizonBooksAnalytics.Report" / "definition" / "report.json"
        self.assertTrue(rpt_json.is_file())

    def test_analytics_page_count(self):
        """Analytics report should have 10 page directories."""
        pages_dir = self.tmpdir / "HorizonBooksAnalytics.Report" / "definition" / "pages"
        page_dirs = [d for d in pages_dir.iterdir() if d.is_dir()]
        self.assertEqual(len(page_dirs), 10)

    def test_forecasting_page_count(self):
        """Forecasting report should have 5 page directories."""
        pages_dir = self.tmpdir / "HorizonBooksForecasting.Report" / "definition" / "pages"
        page_dirs = [d for d in pages_dir.iterdir() if d.is_dir()]
        self.assertEqual(len(page_dirs), 5)

    def test_page_json_valid(self):
        """Each page directory should have a valid page.json."""
        pages_dir = self.tmpdir / "HorizonBooksAnalytics.Report" / "definition" / "pages"
        for page_dir in pages_dir.iterdir():
            if not page_dir.is_dir():
                continue
            page_json = page_dir / "page.json"
            self.assertTrue(page_json.is_file(), f"Missing page.json in {page_dir}")
            data = json.loads(page_json.read_text(encoding="utf-8"))
            self.assertIn("displayName", data)

    def test_theme_file_generated(self):
        theme_dir = (self.tmpdir / "HorizonBooksAnalytics.Report" / "definition" /
                     "StaticResources" / "SharedResources" / "BaseThemes")
        theme_files = list(theme_dir.glob("*.json"))
        self.assertEqual(len(theme_files), 1)

    def test_theme_colors(self):
        theme_path = (self.tmpdir / "HorizonBooksAnalytics.Report" / "definition" /
                      "StaticResources" / "SharedResources" / "BaseThemes" /
                      "HorizonBooksTheme.json")
        data = json.loads(theme_path.read_text(encoding="utf-8"))
        self.assertIn("#1B3A5C", data["dataColors"])  # primary
        self.assertIn("#E8A838", data["dataColors"])  # secondary

    def test_pbip_files(self):
        pbip1 = self.tmpdir / "HorizonBooksAnalytics.pbip"
        pbip2 = self.tmpdir / "HorizonBooksForecasting.pbip"
        self.assertTrue(pbip1.is_file())
        self.assertTrue(pbip2.is_file())

    def test_visual_directories(self):
        """Executive Dashboard should have 7 visual directories."""
        pages_dir = self.tmpdir / "HorizonBooksAnalytics.Report" / "definition" / "pages"
        # Find the Executive Dashboard page by scanning page.json files
        exec_page_dir = None
        for page_dir in pages_dir.iterdir():
            if not page_dir.is_dir():
                continue
            page_json = page_dir / "page.json"
            if page_json.is_file():
                data = json.loads(page_json.read_text(encoding="utf-8"))
                if data.get("displayName") == "Executive Dashboard":
                    exec_page_dir = page_dir
                    break

        self.assertIsNotNone(exec_page_dir, "Executive Dashboard page not found")
        visuals_dir = exec_page_dir / "visuals"
        self.assertTrue(visuals_dir.is_dir())
        visual_dirs = [d for d in visuals_dir.iterdir() if d.is_dir()]
        self.assertEqual(len(visual_dirs), 7)


class TestReportConfig(unittest.TestCase):
    """Test report config structure."""

    def test_two_reports_defined(self):
        config = load_config_file("horizon-books", "reports")
        self.assertEqual(len(config["reports"]), 2)

    def test_analytics_has_10_pages(self):
        config = load_config_file("horizon-books", "reports")
        analytics = config["reports"][0]
        self.assertEqual(len(analytics["pages"]), 10)

    def test_forecasting_has_5_pages(self):
        config = load_config_file("horizon-books", "reports")
        forecasting = config["reports"][1]
        self.assertEqual(len(forecasting["pages"]), 5)


if __name__ == "__main__":
    unittest.main()
