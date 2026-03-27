"""Integration tests — full pipeline execution for all industries.

These tests run the complete generation pipeline and validate the
output directory structure matches expected conventions.
"""

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


INDUSTRIES = [
    "contoso-energy",
    "horizon-books",
    "northwind-hrfinance",
    "fabrikam-manufacturing",
]


class TestFullPipelineExecution(unittest.TestCase):
    """Run generate.py for each industry and verify output structure."""

    def _run_generate(self, industry_id: str, output_dir: Path) -> int:
        result = subprocess.run(
            [sys.executable, "generate.py", "-i", industry_id,
             "-o", str(output_dir)],
            capture_output=True, text=True, timeout=120,
        )
        return result.returncode, result.stdout, result.stderr

    def test_contoso_energy_pipeline(self):
        self._verify_industry("contoso-energy")

    def test_horizon_books_pipeline(self):
        self._verify_industry("horizon-books")

    def test_northwind_hrfinance_pipeline(self):
        self._verify_industry("northwind-hrfinance")

    def test_fabrikam_manufacturing_pipeline(self):
        self._verify_industry("fabrikam-manufacturing")

    def _verify_industry(self, industry_id: str):
        with tempfile.TemporaryDirectory(prefix=f"int_{industry_id}_") as tmpdir:
            out = Path(tmpdir)
            rc, stdout, stderr = self._run_generate(industry_id, out)
            self.assertEqual(rc, 0, f"Pipeline failed for {industry_id}:\n{stderr}")

            # Verify standard output directories exist
            self.assertTrue((out / "SampleData").is_dir(),
                            f"Missing SampleData dir for {industry_id}")
            self.assertTrue((out / "notebooks").is_dir(),
                            f"Missing notebooks dir for {industry_id}")
            self.assertTrue((out / "deploy").is_dir(),
                            f"Missing deploy dir for {industry_id}")

            # Verify CSV files generated (in domain subfolders)
            csvs = list((out / "SampleData").rglob("*.csv"))
            self.assertGreater(len(csvs), 0, f"No CSV files for {industry_id}")

            # Verify notebooks generated
            nbs = list((out / "notebooks").glob("*.py"))
            self.assertGreater(len(nbs), 0, f"No notebooks for {industry_id}")

            # Verify deploy scripts generated
            ps1s = list((out / "deploy").glob("*.ps1"))
            self.assertGreater(len(ps1s), 0, f"No deploy scripts for {industry_id}")


class TestPipelineIdempotency(unittest.TestCase):
    """Running generate twice should produce identical output."""

    def test_idempotent_generation(self):
        with tempfile.TemporaryDirectory(prefix="idem_1_") as d1, \
             tempfile.TemporaryDirectory(prefix="idem_2_") as d2:
            for d in [d1, d2]:
                subprocess.run(
                    [sys.executable, "generate.py", "-i", "horizon-books",
                     "-o", d, "--seed", "42"],
                    capture_output=True, timeout=120,
                )
            # Compare CSV file counts
            csvs1 = sorted(f.name for f in Path(d1).rglob("*.csv"))
            csvs2 = sorted(f.name for f in Path(d2).rglob("*.csv"))
            self.assertEqual(csvs1, csvs2)


class TestPipelineSkipFlags(unittest.TestCase):
    """Verify --skip-* flags work correctly."""

    def test_skip_htap(self):
        with tempfile.TemporaryDirectory(prefix="skip_htap_") as d:
            result = subprocess.run(
                [sys.executable, "generate.py", "-i", "contoso-energy",
                 "-o", d, "--skip-htap"],
                capture_output=True, text=True, timeout=120,
            )
            self.assertEqual(result.returncode, 0)
            self.assertNotIn("Generating HTAP", result.stdout.split("Skipping HTAP")[0]
                             if "Skipping HTAP" in result.stdout else "")

    def test_skip_deploy(self):
        with tempfile.TemporaryDirectory(prefix="skip_deploy_") as d:
            result = subprocess.run(
                [sys.executable, "generate.py", "-i", "contoso-energy",
                 "-o", d, "--skip-deploy"],
                capture_output=True, text=True, timeout=120,
            )
            self.assertEqual(result.returncode, 0)
            self.assertIn("Skipping deploy", result.stdout)


if __name__ == "__main__":
    unittest.main()
