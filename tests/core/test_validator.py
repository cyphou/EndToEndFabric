"""Tests for validator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_all_configs, load_industry_config
from core.csv_generator import generate_all_csvs
from core.notebook_generator import generate_notebooks
from core.dataflow_generator import generate_dataflows
from core.tmdl_generator import generate_semantic_model
from core.report_generator import generate_reports
from core.pipeline_generator import generate_pipeline
from core.deploy_generator import generate_deploy_scripts
from core.validator import validate_output, validate_and_report, ValidationResult


class TestValidatorOnGeneratedOutput(unittest.TestCase):
    """Integration tests: run validator on actual generated demo output."""

    @classmethod
    def setUpClass(cls):
        cls.industry_id = "horizon-books"
        cls.configs = load_all_configs(cls.industry_id)
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_validate_"))

        # Generate core artifacts
        generate_all_csvs(cls.configs["sample_data"], cls.tmpdir, seed=42)
        generate_notebooks(
            cls.configs["industry"],
            cls.configs.get("sample_data"),
            cls.tmpdir,
        )
        generate_dataflows(
            cls.configs["industry"],
            cls.configs["sample_data"],
            cls.tmpdir,
        )
        generate_semantic_model(
            cls.configs["industry"],
            cls.configs["semantic_model"],
            cls.tmpdir,
        )
        generate_reports(
            cls.configs["industry"],
            cls.configs["reports"],
            cls.tmpdir,
        )
        generate_pipeline(
            cls.configs["industry"],
            cls.configs.get("sample_data"),
            cls.tmpdir,
        )
        generate_deploy_scripts(
            cls.configs["industry"],
            cls.configs.get("sample_data"),
            cls.tmpdir,
        )

        cls.results = validate_output(
            cls.configs["industry"], cls.configs, cls.tmpdir)
        cls.summary = validate_and_report(
            cls.configs["industry"], cls.configs, cls.tmpdir)

    def test_results_are_list(self):
        self.assertIsInstance(self.results, list)

    def test_all_results_are_validation_result(self):
        for r in self.results:
            self.assertIsInstance(r, ValidationResult)

    def test_summary_has_required_keys(self):
        for key in ("errors", "warnings", "info", "total", "passed", "results"):
            self.assertIn(key, self.summary)

    def test_no_structure_errors(self):
        """Core artifacts should all be present."""
        structure_errors = [
            r for r in self.results
            if r.severity == "ERROR" and r.category == "structure"
        ]
        self.assertEqual(
            len(structure_errors), 0,
            f"Structure errors: {[r.message for r in structure_errors]}")

    def test_no_metadata_errors(self):
        """JSON schemas and required fields should be correct."""
        metadata_errors = [
            r for r in self.results
            if r.severity == "ERROR" and r.category == "metadata"
        ]
        self.assertEqual(
            len(metadata_errors), 0,
            f"Metadata errors: {[r.message for r in metadata_errors]}")

    def test_no_tmdl_errors(self):
        """TMDL files should be well-formed."""
        tmdl_errors = [
            r for r in self.results
            if r.severity == "ERROR" and r.category == "tmdl"
        ]
        self.assertEqual(
            len(tmdl_errors), 0,
            f"TMDL errors: {[r.message for r in tmdl_errors]}")

    def test_no_placeholder_errors(self):
        """No empty or malformed placeholders."""
        ph_errors = [
            r for r in self.results
            if r.severity == "ERROR" and r.category == "placeholder"
        ]
        self.assertEqual(
            len(ph_errors), 0,
            f"Placeholder errors: {[r.message for r in ph_errors]}")

    def test_passed_flag(self):
        """Overall pass when no errors."""
        errors = [r for r in self.results if r.severity == "ERROR"]
        self.assertEqual(self.summary["passed"], len(errors) == 0)

    def test_validate_and_report_counts(self):
        """Counts should be consistent."""
        self.assertEqual(
            self.summary["total"],
            self.summary["errors"] + self.summary["warnings"] + self.summary["info"])


class TestValidatorEmptyDir(unittest.TestCase):
    """Test validator reports errors on empty output directory."""

    def test_empty_dir_has_errors(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_empty_"))
        configs = load_all_configs("horizon-books")
        results = validate_output(
            configs["industry"], configs, tmpdir)
        errors = [r for r in results if r.severity == "ERROR"]
        self.assertTrue(
            len(errors) > 0,
            "Validator should find errors in empty directory")


class TestValidatorCategories(unittest.TestCase):
    """Test that validation results use known categories."""

    KNOWN_CATEGORIES = {
        "structure", "metadata", "tmdl", "cross-ref", "placeholder", "syntax",
        "completeness"
    }

    def test_all_categories_known(self):
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_cat_"))
        configs = load_all_configs("horizon-books")
        # Generate minimal output
        generate_all_csvs(configs["sample_data"], tmpdir, seed=42)
        results = validate_output(configs["industry"], configs, tmpdir)
        for r in results:
            self.assertIn(
                r.category, self.KNOWN_CATEGORIES,
                f"Unknown category: {r.category} in {r}")


class TestTmdlCrossRef(unittest.TestCase):
    """Sprint 23: TMDL relationship cross-ref validation."""

    @classmethod
    def setUpClass(cls):
        cls.configs = load_all_configs("horizon-books")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_tmdl_xref_"))
        generate_semantic_model(
            cls.configs["industry"],
            cls.configs["semantic_model"],
            cls.tmpdir,
        )
        cls.results = validate_output(
            cls.configs["industry"], cls.configs, cls.tmpdir)

    def test_no_cross_ref_errors_on_relationships(self):
        """All relationship column refs should resolve to existing tables/columns."""
        xref_errors = [
            r for r in self.results
            if r.severity == "ERROR" and r.category == "cross-ref"
               and "fromColumn" in r.message or "toColumn" in r.message
        ]
        self.assertEqual(
            len(xref_errors), 0,
            f"Cross-ref errors: {[r.message for r in xref_errors]}")


class TestPipelineCycleDetection(unittest.TestCase):
    """Sprint 23: Pipeline DAG cycle detection."""

    def test_no_cycle_in_generated_pipeline(self):
        configs = load_all_configs("horizon-books")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_cycle_"))
        generate_pipeline(
            configs["industry"], configs.get("sample_data"), tmpdir)
        results = validate_output(configs["industry"], configs, tmpdir)
        cycle_errors = [
            r for r in results
            if "cycle" in r.message.lower()
        ]
        self.assertEqual(len(cycle_errors), 0, "No cycles expected")

    def test_cycle_detected_in_crafted_pipeline(self):
        """Inject a cyclic pipeline and verify detection."""
        configs = load_all_configs("horizon-books")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_cycle2_"))
        pl_dir = tmpdir / "Pipeline"
        pl_dir.mkdir(parents=True)
        cyclic = {
            "properties": {
                "activities": [
                    {"name": "A", "type": "Notebook",
                     "dependsOn": [{"activity": "B", "dependencyConditions": ["Succeeded"]}]},
                    {"name": "B", "type": "Notebook",
                     "dependsOn": [{"activity": "A", "dependencyConditions": ["Succeeded"]}]},
                ]
            }
        }
        (pl_dir / "pipeline-content.json").write_text(
            json.dumps(cyclic), encoding="utf-8")
        results = validate_output(configs["industry"], configs, tmpdir)
        cycle_errors = [r for r in results if "cycle" in r.message.lower()]
        self.assertEqual(len(cycle_errors), 1, "Should detect cycle")


class TestValidationExport(unittest.TestCase):
    """Sprint 23: Validation report export (JSON + HTML)."""

    @classmethod
    def setUpClass(cls):
        cls.configs = load_all_configs("horizon-books")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_export_"))
        generate_all_csvs(cls.configs["sample_data"], cls.tmpdir, seed=42)
        cls.summary = validate_and_report(
            cls.configs["industry"], cls.configs, cls.tmpdir, export=True)

    def test_json_report_created(self):
        self.assertTrue((self.tmpdir / "validation-report.json").is_file())

    def test_html_report_created(self):
        self.assertTrue((self.tmpdir / "validation-report.html").is_file())

    def test_json_report_valid(self):
        data = json.loads(
            (self.tmpdir / "validation-report.json").read_text(encoding="utf-8"))
        self.assertIn("errors", data)
        self.assertIn("results", data)

    def test_html_report_has_table(self):
        html = (self.tmpdir / "validation-report.html").read_text(encoding="utf-8")
        self.assertIn("<table>", html)
        self.assertIn("Validation Report", html)


class TestConfigCompleteness(unittest.TestCase):
    """Sprint 24: Config completeness validator."""

    def test_valid_configs_no_errors(self):
        """Real configs should have zero completeness errors."""
        configs = load_all_configs("horizon-books")
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_comp_"))
        generate_all_csvs(configs["sample_data"], tmpdir, seed=42)
        results = validate_output(configs["industry"], configs, tmpdir)
        errors = [r for r in results
                  if r.category == "completeness" and r.severity == "ERROR"]
        self.assertEqual(len(errors), 0,
                         f"Expected no completeness errors: {errors}")

    def test_missing_columns_reported(self):
        """Tables with empty columns list should trigger an error."""
        configs = load_all_configs("horizon-books")
        # Inject a table with no columns
        sd = configs["sample_data"]
        sd_data = sd.get("sampleData", sd)
        sd_data["domains"][0]["tables"][0]["columns"] = []
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_comp2_"))
        results = validate_output(configs["industry"], configs, tmpdir)
        col_errors = [r for r in results
                      if r.category == "completeness"
                      and "no columns" in r.message.lower()]
        self.assertGreaterEqual(len(col_errors), 1)

    def test_missing_dax_expression(self):
        """Measures without DAX should trigger an error."""
        configs = load_all_configs("horizon-books")
        sm = configs["semantic_model"]
        sm_inner = sm.get("semanticModel", sm)
        if sm_inner.get("measures"):
            sm_inner["measures"][0]["expression"] = ""
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_comp3_"))
        results = validate_output(configs["industry"], configs, tmpdir)
        dax_errors = [r for r in results
                      if r.category == "completeness"
                      and "no dax" in r.message.lower()]
        self.assertGreaterEqual(len(dax_errors), 1)

    def test_todo_markers_detected(self):
        """TODO markers in config values should produce warnings."""
        configs = load_all_configs("horizon-books")
        configs["industry"]["industry"]["name"] = "TODO: pick a name"
        tmpdir = Path(tempfile.mkdtemp(prefix="fabric_comp4_"))
        results = validate_output(configs["industry"], configs, tmpdir)
        todo_warns = [r for r in results
                      if r.category == "completeness"
                      and "todo" in r.message.lower()]
        self.assertGreaterEqual(len(todo_warns), 1)


if __name__ == "__main__":
    unittest.main()
