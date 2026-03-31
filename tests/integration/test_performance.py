"""Performance benchmark tests — generation must complete under 60s per industry."""

import time
import tempfile
from pathlib import Path
from unittest import TestCase

from core.config_loader import list_industries, load_all_configs, get_output_dir
from core.csv_generator import generate_all_csvs
from core.notebook_generator import generate_notebooks
from core.dataflow_generator import generate_dataflows
from core.tmdl_generator import generate_semantic_model
from core.report_generator import generate_reports
from core.pipeline_generator import generate_pipeline
from core.deploy_generator import generate_deploy_scripts


MAX_SECONDS = 60


class TestPerformanceBenchmark(TestCase):
    """Each industry must generate in under 60 seconds."""

    def _generate_full(self, industry_id: str, output_dir: Path):
        """Run the full generation pipeline for an industry."""
        configs = load_all_configs(industry_id)
        ind_cfg = configs["industry"]

        if configs.get("sample_data"):
            generate_all_csvs(configs["sample_data"], output_dir, seed=42)
        generate_notebooks(ind_cfg, configs.get("sample_data"), output_dir)
        if configs.get("sample_data"):
            generate_dataflows(ind_cfg, configs["sample_data"], output_dir)
        if configs.get("semantic_model"):
            generate_semantic_model(ind_cfg, configs["semantic_model"], output_dir)
        if configs.get("reports"):
            generate_reports(ind_cfg, configs["reports"], output_dir)
        generate_pipeline(ind_cfg, configs.get("sample_data"), output_dir)
        generate_deploy_scripts(ind_cfg, configs.get("sample_data"), output_dir)

    def test_all_industries_under_budget(self):
        """Every industry generates in under 60 seconds."""
        for ind_id in list_industries():
            with tempfile.TemporaryDirectory() as tmp:
                out = Path(tmp)
                start = time.monotonic()
                self._generate_full(ind_id, out)
                elapsed = time.monotonic() - start
                self.assertLess(
                    elapsed, MAX_SECONDS,
                    f"{ind_id} took {elapsed:.1f}s (budget: {MAX_SECONDS}s)",
                )
