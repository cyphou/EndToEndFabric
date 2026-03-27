"""Tests for writeback_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.writeback_generator import generate_writeback


class TestWritebackGenerator(unittest.TestCase):
    """Test writeback artifact generation."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.writeback_config = load_config_file("contoso-energy", "writeback")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_wb_test_"))
        cls.files = generate_writeback(
            cls.industry_config, cls.writeback_config, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.files, list)

    def test_files_generated(self):
        self.assertGreater(len(self.files), 0)

    def test_all_files_exist(self):
        for path in self.files:
            self.assertTrue(path.is_file(), f"Missing: {path}")

    def test_writeback_config_copied(self):
        config_file = self.tmpdir / "Writeback" / "writeback-config.json"
        self.assertTrue(config_file.is_file())
        content = json.loads(config_file.read_text(encoding="utf-8"))
        self.assertIn("writebackConfig", content)

    def test_setup_notebook_generated(self):
        nb = self.tmpdir / "notebooks" / "07_WritebackSetup.py"
        self.assertTrue(nb.is_file())
        content = nb.read_text(encoding="utf-8")
        self.assertIn("CREATE SCHEMA IF NOT EXISTS", content)
        self.assertIn("writeback", content)

    def test_api_notebook_generated(self):
        nb = self.tmpdir / "notebooks" / "08_WritebackAPI.py"
        self.assertTrue(nb.is_file())
        content = nb.read_text(encoding="utf-8")
        self.assertIn("MERGE INTO", content)
        self.assertIn("execute_writeback", content)

    def test_stored_procedures_generated(self):
        sp_dir = self.tmpdir / "Writeback" / "stored_procedures"
        self.assertTrue(sp_dir.is_dir())
        sql_files = list(sp_dir.glob("*.sql"))
        self.assertGreater(len(sql_files), 0)

    def test_stored_procedure_content(self):
        sp_dir = self.tmpdir / "Writeback" / "stored_procedures"
        for sql_path in sp_dir.glob("*.sql"):
            content = sql_path.read_text(encoding="utf-8")
            self.assertIn("MERGE INTO", content)
            self.assertIn("WHEN MATCHED THEN", content)
            self.assertIn("WHEN NOT MATCHED THEN", content)

    def test_setup_creates_all_tables(self):
        nb = self.tmpdir / "notebooks" / "07_WritebackSetup.py"
        content = nb.read_text(encoding="utf-8")
        tables = self.writeback_config["writebackConfig"]["tables"]
        for table in tables:
            self.assertIn(table["name"], content,
                          f"Table {table['name']} not in setup notebook")

    def test_api_has_all_procedures(self):
        nb = self.tmpdir / "notebooks" / "08_WritebackAPI.py"
        content = nb.read_text(encoding="utf-8")
        procs = self.writeback_config["writebackConfig"]["storedProcedures"]
        for proc in procs:
            self.assertIn(proc["name"], content,
                          f"Procedure {proc['name']} not in API notebook")

    def test_procedure_count_matches_config(self):
        sp_dir = self.tmpdir / "Writeback" / "stored_procedures"
        sql_files = list(sp_dir.glob("*.sql"))
        procs = self.writeback_config["writebackConfig"]["storedProcedures"]
        self.assertEqual(len(sql_files), len(procs))


class TestWritebackGeneratorAllIndustries(unittest.TestCase):
    """Test writeback generation works for all industries that have configs."""

    def _test_industry(self, industry_id):
        config = load_industry_config(industry_id)
        wb_config = load_config_file(industry_id, "writeback")
        if wb_config is None:
            self.skipTest(f"No writeback-config.json for {industry_id}")

        tmpdir = Path(tempfile.mkdtemp(prefix=f"fabric_wb_{industry_id}_"))
        files = generate_writeback(config, wb_config, tmpdir)
        self.assertGreater(len(files), 0, f"No files for {industry_id}")

        # Verify setup notebook
        setup = tmpdir / "notebooks" / "07_WritebackSetup.py"
        self.assertTrue(setup.is_file(), f"Missing setup notebook for {industry_id}")

        # Verify API notebook
        api = tmpdir / "notebooks" / "08_WritebackAPI.py"
        self.assertTrue(api.is_file(), f"Missing API notebook for {industry_id}")

    def test_contoso_energy(self):
        self._test_industry("contoso-energy")

    def test_horizon_books(self):
        self._test_industry("horizon-books")

    def test_fabrikam_manufacturing(self):
        self._test_industry("fabrikam-manufacturing")

    def test_northwind_hrfinance(self):
        self._test_industry("northwind-hrfinance")


if __name__ == "__main__":
    unittest.main()
