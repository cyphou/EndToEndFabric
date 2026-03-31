"""Tests for udf_generator module."""

import json
import tempfile
import unittest
from pathlib import Path

from core.config_loader import load_industry_config, load_config_file
from core.udf_generator import generate_udf

INDUSTRIES = [
    "contoso-energy",
    "fabrikam-manufacturing",
    "horizon-books",
    "northwind-hrfinance",
]


class TestUdfGenerator(unittest.TestCase):
    """Test UDF generation for the primary industry (contoso-energy)."""

    @classmethod
    def setUpClass(cls):
        cls.industry_config = load_industry_config("contoso-energy")
        cls.writeback_config = load_config_file("contoso-energy", "writeback")
        cls.tmpdir = Path(tempfile.mkdtemp(prefix="fabric_udf_test_"))
        cls.files = generate_udf(
            cls.industry_config, cls.writeback_config, cls.tmpdir
        )

    def test_returns_list(self):
        self.assertIsInstance(self.files, list)

    def test_three_files_generated(self):
        self.assertEqual(len(self.files), 3)

    def test_all_files_exist(self):
        for path in self.files:
            self.assertTrue(path.is_file(), f"Missing: {path}")

    def test_definition_json_valid(self):
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        content = json.loads(def_path.read_text(encoding="utf-8"))
        self.assertIn("functions", content)
        self.assertIn("connectedDataSources", content)
        self.assertEqual(content["runtime"], "PYTHON")

    def test_functions_json_valid(self):
        meta_path = self.tmpdir / "UserDataFunction" / "resources" / "functions.json"
        content = json.loads(meta_path.read_text(encoding="utf-8"))
        self.assertIn("functionsMetadata", content)
        self.assertEqual(content["runtime"], "PYTHON")

    def test_function_app_has_decorators(self):
        py_path = self.tmpdir / "UserDataFunction" / "function_app.py"
        content = py_path.read_text(encoding="utf-8")
        self.assertIn("@udf.function()", content)
        self.assertIn("import fabric.functions as fn", content)

    def test_definition_has_list_tables(self):
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        content = json.loads(def_path.read_text(encoding="utf-8"))
        names = [f["name"] for f in content["functions"]]
        self.assertIn("list_tables", names)

    def test_definition_has_upsert_and_read_per_table(self):
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        content = json.loads(def_path.read_text(encoding="utf-8"))
        names = set(f["name"] for f in content["functions"])
        tables = self.writeback_config["writebackConfig"]["tables"]
        for table in tables:
            snake = _snake(table["name"])
            self.assertIn(f"upsert_{snake}", names,
                          f"Missing upsert function for {table['name']}")
            self.assertIn(f"read_{snake}", names,
                          f"Missing read function for {table['name']}")

    def test_function_count(self):
        """1 list_tables + 2 per table (upsert + read)."""
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        content = json.loads(def_path.read_text(encoding="utf-8"))
        tables = self.writeback_config["writebackConfig"]["tables"]
        expected = 1 + len(tables) * 2
        self.assertEqual(len(content["functions"]), expected)

    def test_metadata_count_matches_definition(self):
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        meta_path = self.tmpdir / "UserDataFunction" / "resources" / "functions.json"
        defn = json.loads(def_path.read_text(encoding="utf-8"))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        self.assertEqual(len(defn["functions"]),
                         len(meta["functionsMetadata"]))

    def test_metadata_names_match_definition(self):
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        meta_path = self.tmpdir / "UserDataFunction" / "resources" / "functions.json"
        defn = json.loads(def_path.read_text(encoding="utf-8"))
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        def_names = sorted(f["name"] for f in defn["functions"])
        meta_names = sorted(f["name"] for f in meta["functionsMetadata"])
        self.assertEqual(def_names, meta_names)

    def test_function_app_has_all_functions(self):
        py_path = self.tmpdir / "UserDataFunction" / "function_app.py"
        content = py_path.read_text(encoding="utf-8")
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        defn = json.loads(def_path.read_text(encoding="utf-8"))
        for func in defn["functions"]:
            self.assertIn(f"def {func['name']}(", content,
                          f"Function {func['name']} not in function_app.py")

    def test_connected_data_source(self):
        def_path = self.tmpdir / "UserDataFunction" / "definition.json"
        content = json.loads(def_path.read_text(encoding="utf-8"))
        sources = content["connectedDataSources"]
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0]["alias"], "WritebackDB")


class TestUdfGeneratorAllIndustries(unittest.TestCase):
    """Verify all industries produce structurally identical UDF patterns."""

    @classmethod
    def setUpClass(cls):
        cls.results = {}
        for industry_id in INDUSTRIES:
            config = load_industry_config(industry_id)
            wb_config = load_config_file(industry_id, "writeback")
            tmpdir = Path(tempfile.mkdtemp(prefix=f"fabric_udf_{industry_id}_"))
            files = generate_udf(config, wb_config, tmpdir)
            defn = json.loads(
                (tmpdir / "UserDataFunction" / "definition.json")
                .read_text(encoding="utf-8")
            )
            meta = json.loads(
                (tmpdir / "UserDataFunction" / "resources" / "functions.json")
                .read_text(encoding="utf-8")
            )
            py_code = (tmpdir / "UserDataFunction" / "function_app.py").read_text(
                encoding="utf-8"
            )
            tables = wb_config["writebackConfig"]["tables"]
            cls.results[industry_id] = {
                "files": files,
                "defn": defn,
                "meta": meta,
                "py_code": py_code,
                "tables": tables,
            }

    def test_all_generate_three_files(self):
        for industry_id, data in self.results.items():
            self.assertEqual(len(data["files"]), 3,
                             f"{industry_id}: expected 3 files")

    def test_all_have_list_tables(self):
        for industry_id, data in self.results.items():
            names = [f["name"] for f in data["defn"]["functions"]]
            self.assertIn("list_tables", names,
                          f"{industry_id}: missing list_tables")

    def test_all_have_upsert_and_read_per_table(self):
        for industry_id, data in self.results.items():
            names = set(f["name"] for f in data["defn"]["functions"])
            for table in data["tables"]:
                snake = _snake(table["name"])
                self.assertIn(f"upsert_{snake}", names,
                              f"{industry_id}: missing upsert_{snake}")
                self.assertIn(f"read_{snake}", names,
                              f"{industry_id}: missing read_{snake}")

    def test_all_have_same_function_count_pattern(self):
        """Each industry: 1 list_tables + 2 per table."""
        for industry_id, data in self.results.items():
            expected = 1 + len(data["tables"]) * 2
            actual = len(data["defn"]["functions"])
            self.assertEqual(actual, expected,
                             f"{industry_id}: expected {expected} functions, got {actual}")

    def test_definition_and_metadata_counts_match(self):
        for industry_id, data in self.results.items():
            def_count = len(data["defn"]["functions"])
            meta_count = len(data["meta"]["functionsMetadata"])
            self.assertEqual(def_count, meta_count,
                             f"{industry_id}: definition has {def_count} but metadata has {meta_count}")

    def test_definition_and_metadata_names_match(self):
        for industry_id, data in self.results.items():
            def_names = sorted(f["name"] for f in data["defn"]["functions"])
            meta_names = sorted(f["name"] for f in data["meta"]["functionsMetadata"])
            self.assertEqual(def_names, meta_names,
                             f"{industry_id}: definition/metadata name mismatch")

    def test_function_app_has_all_functions(self):
        for industry_id, data in self.results.items():
            for func in data["defn"]["functions"]:
                self.assertIn(f"def {func['name']}(", data["py_code"],
                              f"{industry_id}: {func['name']} missing from function_app.py")

    def test_all_have_writeback_db_connection(self):
        for industry_id, data in self.results.items():
            aliases = [s["alias"] for s in data["defn"]["connectedDataSources"]]
            self.assertIn("WritebackDB", aliases,
                          f"{industry_id}: missing WritebackDB connection")

    def test_all_upsert_metadata_has_sql_binding(self):
        for industry_id, data in self.results.items():
            for entry in data["meta"]["functionsMetadata"]:
                if entry["name"].startswith("upsert_"):
                    binding_types = [b["type"] for b in entry["bindings"]]
                    self.assertIn("FabricItem", binding_types,
                                  f"{industry_id}/{entry['name']}: missing FabricItem binding")

    def test_all_read_metadata_has_sql_binding(self):
        for industry_id, data in self.results.items():
            for entry in data["meta"]["functionsMetadata"]:
                if entry["name"].startswith("read_"):
                    binding_types = [b["type"] for b in entry["bindings"]]
                    self.assertIn("FabricItem", binding_types,
                                  f"{industry_id}/{entry['name']}: missing FabricItem binding")


def _snake(name: str) -> str:
    """Convert PascalCase to snake_case (mirrors udf_generator._snake)."""
    result = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


if __name__ == "__main__":
    unittest.main()
