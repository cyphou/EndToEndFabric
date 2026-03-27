"""Power BI Report (PBIR) generator.

Generates PBIR v4.0 report definitions from reports.json config.
Produces page definitions, visual configs, and theme files.
"""

import json
from pathlib import Path


def generate_reports(industry_config: dict, reports_config: dict,
                     output_dir: Path) -> list[Path]:
    """Generate all Power BI reports for an industry demo.

    Args:
        industry_config: Parsed industry.json content.
        reports_config: Parsed reports.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated report file paths.
    """
    if not reports_config:
        return []

    industry = industry_config.get("industry", {})
    company_name = industry.get("name", "Demo").replace(" ", "")
    theme = industry.get("theme", {})

    reports = reports_config.get("reports", [])
    generated = []

    for report_def in reports:
        report_name = report_def.get("name", f"{company_name}Report")
        report_dir = output_dir / f"{report_name}.Report" / "definition"
        report_dir.mkdir(parents=True, exist_ok=True)

        # Generate report.json (root config)
        root_path = _generate_report_json(report_dir, report_def, company_name, theme)
        generated.append(root_path)

        # Generate pages
        pages = report_def.get("pages", [])
        pages_dir = report_dir / "pages"
        pages_dir.mkdir(exist_ok=True)

        for i, page_def in enumerate(pages):
            page_paths = _generate_page(pages_dir, page_def, i, theme)
            generated.extend(page_paths)

        # Generate theme
        theme_path = _generate_theme(report_dir, theme, company_name)
        generated.append(theme_path)

        # Generate .pbip file
        pbip_path = output_dir / f"{report_name}.pbip"
        _generate_pbip(pbip_path, report_name)
        generated.append(pbip_path)

    return generated


def _generate_report_json(report_dir: Path, report_def: dict,
                          company_name: str, theme: dict) -> Path:
    """Generate the root report.json file."""
    report_name = report_def.get("name", f"{company_name}Report")
    pages = report_def.get("pages", [])

    config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/report/4.0.0/schema.json",
        "id": _pseudo_id(report_name),
        "name": report_name,
        "themeCollection": {
            "baseTheme": {
                "name": f"{company_name}Theme",
                "reportVersionAtImport": "5.56",
                "type": "SharedResources"
            }
        },
        "pageCollection": {
            "pages": [
                {
                    "name": _pseudo_id(page.get("name", f"Page{i+1}")),
                    "displayName": page.get("name", f"Page {i+1}"),
                    "displayOption": page.get("displayOption", "FitToPage"),
                }
                for i, page in enumerate(pages)
            ]
        },
        "defaultDrillFilterOtherVisuals": True,
        "linguisticMetadata": {
            "version": "1.0.0",
            "language": "en-US"
        }
    }

    path = report_dir / "report.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
    return path


def _generate_page(pages_dir: Path, page_def: dict, page_index: int,
                   theme: dict) -> list[Path]:
    """Generate a report page directory with page.json and visual configs."""
    page_name = page_def.get("name", f"Page {page_index + 1}")
    page_id = _pseudo_id(page_name)
    page_dir = pages_dir / page_id
    page_dir.mkdir(parents=True, exist_ok=True)

    generated = []

    # Page config
    page_config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/page/4.0.0/schema.json",
        "name": page_id,
        "displayName": page_name,
        "displayOption": page_def.get("displayOption", "FitToPage"),
        "height": page_def.get("height", 720),
        "width": page_def.get("width", 1280),
        "background": {
            "color": theme.get("background", "#FFFFFF"),
            "transparency": page_def.get("backgroundTransparency", 0)
        }
    }

    page_path = page_dir / "page.json"
    with open(page_path, "w", encoding="utf-8") as f:
        json.dump(page_config, f, indent=2, ensure_ascii=False)
    generated.append(page_path)

    # Generate visuals
    visuals = page_def.get("visuals", [])
    visuals_dir = page_dir / "visuals"
    visuals_dir.mkdir(exist_ok=True)

    for j, visual_def in enumerate(visuals):
        visual_path = _generate_visual(visuals_dir, visual_def, j, theme)
        generated.append(visual_path)

    return generated


def _generate_visual(visuals_dir: Path, visual_def: dict, index: int,
                     theme: dict) -> Path:
    """Generate a single visual config file."""
    visual_type = visual_def.get("type", "card")
    visual_name = visual_def.get("name", f"Visual_{index + 1}")
    visual_id = _pseudo_id(visual_name)

    visual_dir = visuals_dir / visual_id
    visual_dir.mkdir(parents=True, exist_ok=True)

    config = {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/report/definition/visual/4.0.0/schema.json",
        "name": visual_id,
        "visual": {
            "visualType": _map_visual_type(visual_type),
            "objects": {},
            "visualContainerObjects": {
                "title": [{
                    "properties": {
                        "show": {"expr": {"Literal": {"Value": "true"}}},
                        "text": {"expr": {"Literal": {"Value": f"'{visual_name}'"}}}
                    }
                }]
            }
        },
        "position": {
            "x": visual_def.get("x", 0),
            "y": visual_def.get("y", 0),
            "width": visual_def.get("width", 300),
            "height": visual_def.get("height", 200),
            "z": index
        }
    }

    # Add data roles mapping
    data_roles = visual_def.get("dataRoles", {})
    if data_roles:
        config["visual"]["queries"] = _build_query_refs(data_roles)

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
            "#6C757D",
            "#28A745",
            "#17A2B8",
            "#FFC107",
        ],
        "background": theme.get("background", "#FFFFFF"),
        "foreground": "#333333",
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

    theme_dir = report_dir / "StaticResources" / "SharedResources" / "BaseThemes"
    theme_dir.mkdir(parents=True, exist_ok=True)
    path = theme_dir / f"{company_name}Theme.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(theme_config, f, indent=2, ensure_ascii=False)
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
    # Simplified — actual PBIR queries are more complex
    return [{"queryRef": role} for role in data_roles.values() if role]


def _pseudo_id(seed: str) -> str:
    """Generate a deterministic ID string."""
    import hashlib
    return hashlib.md5(seed.encode()).hexdigest()[:16]
