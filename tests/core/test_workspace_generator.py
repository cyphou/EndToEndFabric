"""Tests for workspace_generator — Task Flow definition + workspace icon SVG."""

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from core.workspace_generator import (
    generate_workspace_artifacts,
    _pick_symbol,
    _initials,
    _build_icon_svg,
    _ICON_PATHS,
    _INDUSTRY_SYMBOL_MAP,
)

# ── Minimal fixtures ────────────────────────────────────────────────────────

_INDUSTRY = {
    "industry": {
        "id": "test-industry",
        "name": "TestIndustry",
        "displayName": "Test Industry",
        "description": "A test industry for testing.",
        "domains": ["Sales", "Finance"],
        "theme": {
            "primary": "#1565C0",
            "secondary": "#E65100",
        },
    },
    "fabricArtifacts": {
        "workspacePrefix": "TestIndustry",
        "lakehouses": {"bronze": "BronzeLH", "silver": "SilverLH", "gold": "GoldLH"},
    },
}

_SAMPLE_DATA = {
    "sampleData": {
        "description": "Test sample data",
        "domains": [
            {"name": "Sales",   "folder": "sales",   "tables": [{"name": "FactSales",    "fileName": "FactSales.csv",    "rowCount": 10, "columns": []}]},
            {"name": "Finance", "folder": "finance", "tables": [{"name": "FactFinance",  "fileName": "FactFinance.csv",  "rowCount": 10, "columns": []}]},
        ],
    }
}

_CONTOSO_INDUSTRY = {
    "industry": {
        "id": "contoso-energy",
        "name": "ContosoEnergy",
        "displayName": "Contoso Energy",
        "description": "Energy utility.",
        "domains": ["Generation", "GridOperations", "CustomerBilling"],
        "theme": {"primary": "#2E7D32", "secondary": "#FF6F00"},
    },
    "fabricArtifacts": {
        "workspacePrefix": "ContosoEnergy",
        "lakehouses": {"bronze": "BronzeLH", "silver": "SilverLH", "gold": "GoldLH"},
    },
}


class TestWorkspaceArtifactsOutput(unittest.TestCase):

    def setUp(self):
        self._tmpdir = TemporaryDirectory()
        self.out = Path(self._tmpdir.name)

    def tearDown(self):
        self._tmpdir.cleanup()

    # ── File creation ──────────────────────────────────────────────────────

    def test_returns_two_files(self):
        paths = generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        self.assertEqual(len(paths), 2)

    def test_taskflow_file_created(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        self.assertTrue((self.out / "TaskFlow" / "taskflow-definition.json").exists())

    def test_icon_file_created(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        self.assertTrue((self.out / "WorkspaceIcon" / "icon.svg").exists())

    # ── Task Flow structure ────────────────────────────────────────────────

    def test_taskflow_is_valid_json(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        raw = (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        data = json.loads(raw)
        self.assertIsInstance(data, dict)

    def test_taskflow_has_required_keys(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        for key in ("schemaVersion", "metadata", "layout", "nodes", "edges"):
            self.assertIn(key, data)

    def test_taskflow_nodes_include_lakehouses(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        self.assertIn("bronze-lh", node_ids)
        self.assertIn("silver-lh", node_ids)
        self.assertIn("gold-lh", node_ids)

    def test_taskflow_has_dataflow_per_domain(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        self.assertIn("df-sales", node_ids)
        self.assertIn("df-finance", node_ids)

    def test_taskflow_edges_are_non_empty(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        self.assertGreater(len(data["edges"]), 0)

    def test_taskflow_edges_reference_valid_nodes(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        for edge in data["edges"]:
            self.assertIn(edge["source"], node_ids,
                          f"Edge source '{edge['source']}' not in nodes")
            self.assertIn(edge["target"], node_ids,
                          f"Edge target '{edge['target']}' not in nodes")

    def test_skip_htap_removes_eventhouse_node(self):
        generate_workspace_artifacts(
            _INDUSTRY, _SAMPLE_DATA, self.out, skip_htap=True
        )
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        self.assertNotIn("eventhouse", node_ids)
        self.assertNotIn("nb05", node_ids)

    def test_skip_forecast_removes_nb04_node(self):
        generate_workspace_artifacts(
            _INDUSTRY, _SAMPLE_DATA, self.out, skip_forecast=True
        )
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        self.assertNotIn("nb04", node_ids)

    def test_skip_writeback_removes_writeback_node(self):
        generate_workspace_artifacts(
            _INDUSTRY, _SAMPLE_DATA, self.out, skip_writeback=True
        )
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        self.assertNotIn("nb07-09", node_ids)

    def test_full_pipeline_includes_all_optional_nodes(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        node_ids = {n["id"] for n in data["nodes"]}
        for nid in ("nb04", "nb05", "eventhouse", "nb07-09"):
            self.assertIn(nid, node_ids)

    def test_taskflow_metadata_contains_industry_id(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        self.assertEqual(data["metadata"]["industry"], "test-industry")

    # ── Workspace icon ─────────────────────────────────────────────────────

    def test_icon_contains_primary_color(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        svg = (self.out / "WorkspaceIcon" / "icon.svg").read_text()
        self.assertIn("#1565C0", svg)

    def test_icon_contains_secondary_color(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        svg = (self.out / "WorkspaceIcon" / "icon.svg").read_text()
        self.assertIn("#E65100", svg)

    def test_icon_contains_abbreviation(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        svg = (self.out / "WorkspaceIcon" / "icon.svg").read_text()
        # "Test Industry" → "TI"
        self.assertIn("TI", svg)

    def test_icon_starts_with_xml_declaration(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        svg = (self.out / "WorkspaceIcon" / "icon.svg").read_text()
        self.assertTrue(svg.startswith("<?xml"))

    # ── No sample data ─────────────────────────────────────────────────────

    def test_works_without_sample_data(self):
        paths = generate_workspace_artifacts(_INDUSTRY, None, self.out)
        self.assertEqual(len(paths), 2)
        data = json.loads(
            (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        )
        # Domains fall back to industry.domains
        node_ids = {n["id"] for n in data["nodes"]}
        self.assertIn("df-sales", node_ids)

    # ── Known industries ───────────────────────────────────────────────────

    def test_contoso_energy_uses_energy_symbol(self):
        generate_workspace_artifacts(_CONTOSO_INDUSTRY, None, self.out)
        svg = (self.out / "WorkspaceIcon" / "icon.svg").read_text()
        # Energy symbol path must appear
        self.assertIn(_ICON_PATHS["energy"], svg)

    def test_contoso_energy_initials_are_CE(self):
        generate_workspace_artifacts(_CONTOSO_INDUSTRY, None, self.out)
        svg = (self.out / "WorkspaceIcon" / "icon.svg").read_text()
        self.assertIn("CE", svg)

    # ── Idempotency ────────────────────────────────────────────────────────

    def test_idempotent(self):
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        first = (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        generate_workspace_artifacts(_INDUSTRY, _SAMPLE_DATA, self.out)
        second = (self.out / "TaskFlow" / "taskflow-definition.json").read_text()
        self.assertEqual(first, second)


class TestHelpers(unittest.TestCase):

    def test_pick_symbol_known_industries(self):
        for industry_id, expected in _INDUSTRY_SYMBOL_MAP.items():
            self.assertEqual(_pick_symbol(industry_id, []), expected)

    def test_pick_symbol_domain_fallback_energy(self):
        self.assertEqual(_pick_symbol("unknown", ["Power", "Grid"]), "energy")

    def test_pick_symbol_domain_fallback_manufacturing(self):
        self.assertEqual(_pick_symbol("unknown", ["production", "Quality"]), "manufacturing")

    def test_pick_symbol_default_for_unknown(self):
        self.assertEqual(_pick_symbol("unknown-industry", []), "default")

    def test_initials_two_words(self):
        self.assertEqual(_initials("Horizon Books"), "HB")

    def test_initials_single_word(self):
        self.assertEqual(_initials("Fabrikam"), "FA")

    def test_initials_hyphenated(self):
        self.assertEqual(_initials("contoso-energy"), "CE")

    def test_build_icon_svg_contains_path(self):
        path_d = _ICON_PATHS["energy"]
        svg = _build_icon_svg("#123456", "#654321", path_d, "CE", "Contoso Energy")
        self.assertIn(path_d, svg)

    def test_all_icon_paths_defined(self):
        for key in ("energy", "book", "finance", "manufacturing", "default"):
            self.assertIn(key, _ICON_PATHS)
            self.assertGreater(len(_ICON_PATHS[key]), 0)


if __name__ == "__main__":
    unittest.main()
