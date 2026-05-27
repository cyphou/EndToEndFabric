"""Power BI Report (PBIR) generator.

Generates PBIR v4.0 report definitions from reports.json config.
Produces page definitions, visual configs, and theme files.
"""

import json
from pathlib import Path

# PBIR base theme defaults — override via industry.json "fabricArtifacts.pbirTheme"
DEFAULT_BASE_THEME_NAME = "CY24SU06"
DEFAULT_REPORT_VERSION_AT_IMPORT = "5.56"


def generate_reports(industry_config: dict, reports_config: dict,
                     output_dir: Path,
                     semantic_model_config: dict | None = None) -> list[Path]:
    """Generate all Power BI reports for an industry demo.

    Args:
        industry_config: Parsed industry.json content.
        reports_config: Parsed reports.json content.
        output_dir: Demo output root directory.
        semantic_model_config: Parsed semantic-model.json (used for
            measure → table resolution in visual data bindings).

    Returns:
        List of generated report file paths.
    """
    if not reports_config:
        return []

    industry = industry_config.get("industry", {})
    company_name = industry.get("name", "Demo").replace(" ", "")
    theme = industry.get("theme", {})
    artifacts = industry_config.get("fabricArtifacts", {})
    pbir_theme = artifacts.get("pbirTheme", None)

    # Build measure → table lookup from semantic model config
    measure_table_map = _build_measure_table_map(semantic_model_config)

    reports = reports_config.get("reports", [])
    generated = []

    for report_def in reports:
        report_name = report_def.get("name", f"{company_name}Report")
        report_dir = output_dir / f"{report_name}.Report" / "definition"
        report_dir.mkdir(parents=True, exist_ok=True)

        # Generate report.json (root config)
        root_path = _generate_report_json(report_dir, report_def, company_name, theme, pbir_theme)
        generated.append(root_path)

        # Generate pages
        pages = report_def.get("pages", [])
        pages_dir = report_dir / "pages"
        pages_dir.mkdir(exist_ok=True)

        for i, page_def in enumerate(pages):
            page_paths = _generate_page(pages_dir, page_def, i, theme,
                                        measure_table_map)
            generated.extend(page_paths)

        # Generate theme
        theme_path = _generate_theme(report_dir, theme, company_name)
        generated.append(theme_path)

        # Generate definition.pbir (semantic model binding) — sits at .Report/ root
        report_root = output_dir / f"{report_name}.Report"
        sm_ref = report_def.get("semanticModel", "default")
        pbir_path = _generate_definition_pbir(report_root, company_name, sm_ref)
        generated.append(pbir_path)

        # Generate version.json (required by Fabric report import)
        version_path = _generate_version_json(report_dir)
        generated.append(version_path)

        # Generate .pbip file
        pbip_path = output_dir / f"{report_name}.pbip"
        _generate_pbip(pbip_path, report_name)
        generated.append(pbip_path)

    return generated


def _generate_report_json(report_dir: Path, report_def: dict,
                          company_name: str, theme: dict,
                          pbir_theme: dict | None = None) -> Path:
    """Generate the root report.json file."""
    report_name = report_def.get("name", f"{company_name}Report")
    pages = report_def.get("pages", [])

    pbir = pbir_theme or {}
    base_theme_name = pbir.get("baseThemeName", DEFAULT_BASE_THEME_NAME)
    report_version = pbir.get("reportVersionAtImport", DEFAULT_REPORT_VERSION_AT_IMPORT)
    custom_theme_name = f"{company_name}Theme"

    config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/1.0.0/schema.json",
        "themeCollection": {
            "baseTheme": {
                "name": base_theme_name,
                "reportVersionAtImport": report_version,
                "type": "SharedResources"
            },
            "customTheme": {
                "name": custom_theme_name,
                "reportVersionAtImport": report_version,
                "type": "RegisteredResources"
            }
        },
        "resourcePackages": [
            {
                "name": "RegisteredResources",
                "type": "RegisteredResources",
                "items": [
                    {
                        "name": f"{custom_theme_name}.json",
                        "path": f"{custom_theme_name}.json",
                        "type": "CustomTheme"
                    }
                ]
            }
        ],
        "layoutOptimization": "None"
    }

    path = report_dir / "report.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    return path


def _generate_page(pages_dir: Path, page_def: dict, page_index: int,
                   theme: dict, measure_table_map: dict) -> list[Path]:
    """Generate a report page directory with page.json and visual configs."""
    page_name = page_def.get("name", f"Page {page_index + 1}")
    page_id = _pseudo_id(page_name)
    page_dir = pages_dir / page_id
    page_dir.mkdir(parents=True, exist_ok=True)

    generated = []

    page_width = page_def.get("width", 1280)
    page_height = page_def.get("height", 720)

    # Page config
    page_config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/1.0.0/schema.json",
        "name": page_id,
        "displayName": page_name,
        "displayOption": page_def.get("displayOption", "FitToPage"),
        "height": page_height,
        "width": page_width
    }

    page_path = page_dir / "page.json"
    with open(page_path, "w", encoding="utf-8") as f:
        json.dump(page_config, f, indent=2, ensure_ascii=False)
    generated.append(page_path)

    # Generate visuals with layout
    visuals = page_def.get("visuals", [])
    visuals_dir = page_dir / "visuals"
    visuals_dir.mkdir(exist_ok=True)

    layout = _compute_layout(visuals, page_width, page_height)

    for j, visual_def in enumerate(visuals):
        pos = layout[j] if j < len(layout) else {"x": 0, "y": 0, "width": 300, "height": 200}
        visual_path = _generate_visual(visuals_dir, visual_def, j, theme,
                                       measure_table_map, pos)
        generated.append(visual_path)

    return generated


def _generate_visual(visuals_dir: Path, visual_def: dict, index: int,
                     theme: dict, measure_table_map: dict,
                     pos: dict) -> Path:
    """Generate a single visual config file with proper PBIR data bindings."""
    visual_type = visual_def.get("type", "card")
    visual_name = visual_def.get("name", f"Visual_{index + 1}")
    visual_id = _pseudo_id(visual_name)

    visual_dir = visuals_dir / visual_id
    visual_dir.mkdir(parents=True, exist_ok=True)

    pbir_type = _map_visual_type(visual_type)

    # Build query state from visual definition
    query_state, title_text = _build_visual_query(
        visual_def, visual_type, measure_table_map
    )

    visual_node = {
        "visualType": pbir_type,
        "visualContainerObjects": {
            "title": [{
                "properties": {
                    "show": {"expr": {"Literal": {"Value": "true"}}},
                    "text": {"expr": {"Literal": {"Value": f"'{title_text}'"}}}
                }
            }]
        }
    }

    if query_state:
        visual_node["query"] = {"queryState": query_state}

    config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visualContainer/1.0.0/schema.json",
        "name": visual_id,
        "visual": visual_node,
        "position": {
            "x": pos["x"],
            "y": pos["y"],
            "width": pos["width"],
            "height": pos["height"],
            "z": index
        }
    }

    path = visual_dir / "visual.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    return path


def _generate_theme(report_dir: Path, theme: dict, company_name: str) -> Path:
    """Generate a Power BI theme JSON file."""
    theme_config = {
        "name": f"{company_name}Theme",
        "dataColors": [
            theme.get("primary", "#0078D4"),
            theme.get("secondary", "#FFB81C"),
            theme.get("accent1", "#4A90D9"),
            theme.get("accent2", "#E74C3C"),
            theme.get("neutral", "#6C757D"),
            theme.get("positive", "#28A745"),
            theme.get("info", "#17A2B8"),
            theme.get("warning", "#FFC107"),
        ],
        "background": theme.get("background", "#FFFFFF"),
        "foreground": theme.get("foreground", "#333333"),
        "tableAccent": theme.get("primary", "#0078D4"),
        "visualStyles": {
            "*": {
                "*": {
                    "general": [{
                        "responsive": True
                    }]
                }
            }
        }
    }

    theme_dir = report_dir / "StaticResources" / "RegisteredResources"
    theme_dir.mkdir(parents=True, exist_ok=True)
    path = theme_dir / f"{company_name}Theme.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(theme_config, f, indent=2, ensure_ascii=False)
    return path


def _generate_definition_pbir(report_dir: Path, company_name: str,
                               semantic_model: str = "default") -> Path:
    """Generate the definition.pbir file that binds the report to a semantic model.

    Args:
        report_dir: The .Report/ directory root.
        company_name: Company name (used for model naming).
        semantic_model: Which model to bind — "default" or "writeback".
    """
    if semantic_model == "writeback":
        model_token = "{{WRITEBACK_MODEL_ID}}"
    else:
        model_token = "{{SEMANTIC_MODEL_ID}}"

    config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definitionProperties/2.0.0/schema.json",
        "version": "4.0",
        "datasetReference": {
            "byConnection": {
                "connectionString": f"semanticmodelid={model_token}"
            }
        }
    }
    path = report_dir / "definition.pbir"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    return path


def _generate_pbip(path: Path, report_name: str):
    """Generate a .pbip project file."""
    config = {
        "version": "1.0",
        "artifacts": [
            {
                "report": {
                    "path": f"{report_name}.Report"
                }
            }
        ],
        "settings": {
            "enableAutoRecovery": True
        }
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)


def _generate_version_json(report_dir: Path) -> Path:
    """Generate version.json required by Fabric PBIR import."""
    config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/versionMetadata/1.0.0/schema.json",
        "version": "1.0.0"
    }
    path = report_dir / "version.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    return path


def _map_visual_type(simple_type: str) -> str:
    """Map simple visual type names to PBIR visual type identifiers."""
    mapping = {
        "card": "card",
        "kpi": "multiRowCard",
        "bar": "clusteredBarChart",
        "column": "clusteredColumnChart",
        "stacked_bar": "stackedBarChart",
        "stacked_column": "100PercentStackedColumnChart",
        "line": "lineChart",
        "area": "areaChart",
        "combo": "lineClusteredColumnComboChart",
        "pie": "pieChart",
        "donut": "donutChart",
        "treemap": "treemap",
        "map": "map",
        "filled_map": "filledMap",
        "table": "tableEx",
        "matrix": "pivotTable",
        "gauge": "gauge",
        "funnel": "funnel",
        "waterfall": "waterfallChart",
        "scatter": "scatterChart",
        "slicer": "slicer",
        "text": "textbox",
        "image": "image",
        "shape": "shape",
        "decomposition_tree": "decompositionTreeVisual",
        "key_influencers": "keyInfluencers",
        "ribbon": "ribbonChart",
    }
    return mapping.get(simple_type, simple_type)


def _build_query_refs(data_roles: dict) -> list[dict]:
    """Build minimal query references from data role mappings."""
    return [{"queryRef": role} for role in data_roles.values() if role]


def _pseudo_id(seed: str) -> str:
    """Generate a deterministic ID string."""
    import hashlib
    return hashlib.md5(seed.encode(), usedforsecurity=False).hexdigest()[:16]


# ── Measure / column resolution helpers ──────────────────────────────────

def _build_measure_table_map(sm_config: dict | None) -> dict:
    """Build {measure_name: table_name} from semantic-model.json."""
    if not sm_config:
        return {}
    # Measures may be at root or nested under "semanticModel"
    inner = sm_config.get("semanticModel", sm_config)
    lookup = {}
    for m in inner.get("measures", []):
        lookup[m["name"]] = m.get("table", "")
    return lookup


def _parse_field_ref(ref: str) -> tuple[str, str]:
    """Parse 'Table[Column]' → ('Table', 'Column').

    Falls back to ('', ref) when no bracket syntax is found.
    """
    import re
    m = re.match(r'^(.+?)\[(.+?)\]$', ref)
    if m:
        return m.group(1), m.group(2)
    return "", ref


def _resolve_measure(name: str, measure_table_map: dict) -> tuple[str, str]:
    """Resolve a measure name to (table, measure_name).

    If the name is in Table[Measure] format, parse directly.
    Otherwise look up in the measure_table_map.
    """
    table, prop = _parse_field_ref(name)
    if table:
        return table, prop
    table = measure_table_map.get(name, "")
    return table, name


# ── PBIR field / projection builders ─────────────────────────────────────

def _make_column_field(entity: str, prop: str) -> dict:
    return {
        "Column": {
            "Expression": {"SourceRef": {"Entity": entity}},
            "Property": prop
        }
    }


def _make_measure_field(entity: str, prop: str) -> dict:
    return {
        "Measure": {
            "Expression": {"SourceRef": {"Entity": entity}},
            "Property": prop
        }
    }


def _projection(field_dict: dict, query_ref: str, native_ref: str) -> dict:
    return {
        "field": field_dict,
        "queryRef": query_ref,
        "nativeQueryRef": native_ref
    }


# ── Visual query builder ─────────────────────────────────────────────────

def _build_visual_query(visual_def: dict, visual_type: str,
                        measure_table_map: dict
                        ) -> tuple[dict | None, str]:
    """Return (queryState, title) for a visual definition."""

    query_state: dict = {}

    def _add_column(role: str, ref: str):
        """Add a column reference to query_state."""
        table, col = _parse_field_ref(ref)
        if not table:
            return
        qr = f"{table}.{col}"
        proj = _projection(_make_column_field(table, col), qr, col)
        query_state.setdefault(role, {"projections": []})
        query_state[role]["projections"].append(proj)

    def _add_measure(role: str, name: str):
        """Add a measure reference to query_state."""
        table, prop = _resolve_measure(name, measure_table_map)
        if not table:
            return
        qr = f"{table}.{prop}"
        proj = _projection(_make_measure_field(table, prop), qr, prop)
        query_state.setdefault(role, {"projections": []})
        query_state[role]["projections"].append(proj)

    title = visual_def.get("name", "")

    # --- Card: single measure --------------------------------------------------
    if visual_type == "card" and "measure" in visual_def:
        _add_measure("Values", visual_def["measure"])
        title = title or visual_def["measure"]

    # --- Gauge: value + optional target ----------------------------------------
    elif visual_type == "gauge" and "value" in visual_def:
        _add_measure("Y", visual_def["value"])
        title = title or visual_def["value"]

    # --- Chart types with axis + values ----------------------------------------
    elif visual_type in ("lineChart", "areaChart", "clusteredBarChart",
                         "stackedBarChart", "combo", "line", "area",
                         "bar", "column", "stacked_bar", "stacked_column",
                         "ribbon", "waterfall"):
        axis_ref = visual_def.get("axis", "")
        if axis_ref:
            _add_column("Category", axis_ref)
        for val in visual_def.get("values", []):
            _add_measure("Y", val)
        title = title or ", ".join(visual_def.get("values", [""]))

    # --- Pie / Donut / Treemap: legend/category + values ----------------------
    elif visual_type in ("donutChart", "pieChart", "donut", "pie",
                         "treemap"):
        legend = visual_def.get("legend", visual_def.get("category", ""))
        if legend:
            _add_column("Category", legend)
        for val in visual_def.get("values", []):
            _add_measure("Y", val)
        title = title or ", ".join(visual_def.get("values", [""]))

    # --- Map: location + size/color -------------------------------------------
    elif visual_type in ("map", "filled_map"):
        loc = visual_def.get("location", "")
        if loc:
            _add_column("Category", loc)
        size_m = visual_def.get("size", visual_def.get("color", ""))
        if size_m:
            _add_measure("Size", size_m)
        title = title or "Map"

    # --- Table: columns list --------------------------------------------------
    elif visual_type in ("table", "tableEx"):
        for col_ref in visual_def.get("columns", []):
            table, col = _parse_field_ref(col_ref)
            if table:
                _add_column("Values", col_ref)
            else:
                _add_measure("Values", col_ref)
        title = title or "Detail Table"

    # --- Matrix: rows + columns + values --------------------------------------
    elif visual_type in ("matrix", "pivotTable"):
        for r in visual_def.get("rows", []):
            _add_column("Rows", r)
        for c in visual_def.get("columns", []):
            _add_column("Columns", c)
        for v in visual_def.get("values", []):
            _add_measure("Values", v)
        title = title or "Matrix"

    # --- KPI / multiRowCard ---------------------------------------------------
    elif visual_type == "kpi" and "measure" in visual_def:
        _add_measure("Values", visual_def["measure"])
        title = title or visual_def["measure"]

    return (query_state or None, title or f"Visual {visual_type}")


# ── Layout engine ─────────────────────────────────────────────────────────

# Default sizes per visual type (width, height)
_DEFAULT_SIZES = {
    "card": (200, 120),
    "kpi": (400, 200),
    "lineChart": (580, 320),
    "line": (580, 320),
    "areaChart": (580, 320),
    "area": (580, 320),
    "clusteredBarChart": (580, 320),
    "bar": (580, 320),
    "column": (580, 320),
    "stackedBarChart": (580, 320),
    "stacked_bar": (580, 320),
    "stacked_column": (580, 320),
    "combo": (580, 320),
    "donutChart": (380, 320),
    "donut": (380, 320),
    "pieChart": (380, 320),
    "pie": (380, 320),
    "table": (780, 360),
    "tableEx": (780, 360),
    "matrix": (780, 360),
    "pivotTable": (780, 360),
    "treemap": (480, 320),
    "gauge": (280, 240),
    "map": (580, 380),
    "filled_map": (580, 380),
    "scatter": (480, 320),
    "waterfall": (580, 320),
    "ribbon": (580, 320),
    "text": (400, 60),
    "image": (200, 200),
}

_MARGIN = 20
_PAGE_WIDTH = 1280
_PAGE_HEIGHT = 720


def _compute_layout(visuals: list[dict], page_w: int = _PAGE_WIDTH,
                    page_h: int = _PAGE_HEIGHT) -> list[dict]:
    """Compute non-overlapping positions for visuals using a simple row packer."""
    usable_w = page_w - 2 * _MARGIN
    positions = []
    cursor_x = _MARGIN
    cursor_y = _MARGIN
    row_height = 0

    for v in visuals:
        vtype = v.get("type", "card")
        dw, dh = _DEFAULT_SIZES.get(vtype, (300, 200))
        w = v.get("width", dw)
        h = v.get("height", dh)

        # Wrap to next row if needed
        if cursor_x + w > page_w - _MARGIN and cursor_x > _MARGIN:
            cursor_x = _MARGIN
            cursor_y += row_height + _MARGIN
            row_height = 0

        positions.append({"x": cursor_x, "y": cursor_y, "width": w, "height": h})
        cursor_x += w + _MARGIN
        row_height = max(row_height, h)

    return positions

