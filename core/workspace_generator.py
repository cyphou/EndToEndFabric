"""Workspace generator — produces Fabric Task Flow definition and workspace icon SVG.

Generates from industry.json + sample-data.json:
  TaskFlow/taskflow-definition.json — architecture DAG (nodes + edges + layout)
  WorkspaceIcon/icon.svg            — SVG workspace icon branded per industry theme

The Task Flow JSON follows the Fabric item definition format compatible with the
Fabric REST API (POST /workspaces/{id}/items, type="TaskFlow").

The workspace icon SVG (200×200) uses the industry's primary theme color and a
domain-specific symbol, and can be uploaded via the Fabric REST API
(POST /workspaces/{id}/workspaceIcon).
"""

import json
from pathlib import Path


# ─── Icon symbol paths (24×24 design space) ───────────────────────────────────
# Paths are filled SVG shapes centered in the 24×24 viewBox.
_ICON_PATHS: dict[str, str] = {
    # Lightning bolt — energy / power
    "energy": "M13 2L5 13H11L9 22L19 11H13L13 2Z",
    # Open book (two pages + spine) — publishing / content
    "book": "M4 2H11V22H4V2ZM13 2H20V22H13V2ZM11 11H13V13H11Z",
    # Bar chart columns — finance / HR analytics
    "finance": "M3 20V8H7V20H3ZM9 20V3H13V20H9ZM15 20V12H19V20H15Z",
    # Gear outline — manufacturing / industrial
    "manufacturing": (
        "M12 8C9.8 8 8 9.8 8 12C8 14.2 9.8 16 12 16C14.2 16 16 14.2 16 12C16 9.8 14.2 8 12 8Z"
        "M19.4 13C19.5 12.7 19.5 12.3 19.4 12L21.5 10.4C21.7 10.2 21.7 9.9 21.6 9.7"
        "L19.6 6.3C19.5 6.1 19.2 6 19 6.1L16.5 7.1C16 6.7 15.5 6.4 14.9 6.2"
        "L14.5 3.5C14.5 3.2 14.2 3 13.9 3H10.1C9.8 3 9.6 3.2 9.5 3.5"
        "L9.1 6.2C8.5 6.4 8 6.8 7.5 7.1L5 6.1C4.7 6 4.4 6.1 4.3 6.3"
        "L2.3 9.7C2.2 9.9 2.2 10.2 2.4 10.4L4.5 12C4.4 12.3 4.4 12.7 4.5 13"
        "L2.4 14.6C2.2 14.8 2.2 15.1 2.3 15.3L4.3 18.7C4.4 18.9 4.7 19 5 18.9"
        "L7.5 17.9C8 18.3 8.5 18.6 9.1 18.8L9.5 21.5C9.6 21.8 9.8 22 10.1 22"
        "H13.9C14.2 22 14.5 21.8 14.5 21.5L14.9 18.8C15.5 18.6 16 18.2 16.5 17.9"
        "L19 18.9C19.3 19 19.6 18.9 19.7 18.7L21.7 15.3C21.8 15.1 21.8 14.8 21.6 14.6Z"
    ),
    # Stacked layers — generic / default
    "default": "M12 3L2 8L12 13L22 8L12 3ZM2 13L12 18L22 13M2 18L12 23L22 18",
}

# ─── Industry → symbol mapping ────────────────────────────────────────────────
_INDUSTRY_SYMBOL_MAP: dict[str, str] = {
    "contoso-energy":         "energy",
    "horizon-books":          "book",
    "northwind-hrfinance":    "finance",
    "fabrikam-manufacturing": "manufacturing",
}


# ─── Public entry point ───────────────────────────────────────────────────────

def generate_workspace_artifacts(
    industry_config: dict,
    sample_data_config: dict | None,
    output_dir: Path,
    *,
    skip_forecast: bool = False,
    skip_htap: bool = False,
    skip_writeback: bool = False,
) -> list[Path]:
    """Generate Task Flow definition and workspace SVG icon.

    Args:
        industry_config:    Parsed industry.json content.
        sample_data_config: Parsed sample-data.json, or None.
        output_dir:         Demo output root directory.
        skip_forecast:      Exclude forecast nodes/edges when True.
        skip_htap:          Exclude HTAP/Eventhouse nodes/edges when True.
        skip_writeback:     Exclude writeback nodes/edges when True.

    Returns:
        List of created file paths.
    """
    created: list[Path] = []
    created.extend(
        _generate_taskflow(
            industry_config,
            sample_data_config,
            output_dir,
            skip_forecast=skip_forecast,
            skip_htap=skip_htap,
            skip_writeback=skip_writeback,
        )
    )
    created.extend(_generate_workspace_icon(industry_config, output_dir))
    return created


# ─── Task Flow ────────────────────────────────────────────────────────────────

def _generate_taskflow(
    industry_config: dict,
    sample_data_config: dict | None,
    output_dir: Path,
    *,
    skip_forecast: bool,
    skip_htap: bool,
    skip_writeback: bool,
) -> list[Path]:
    """Build taskflow-definition.json representing the medallion architecture DAG."""
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    lakehouses = artifacts.get("lakehouses", {})
    bronze_lh = lakehouses.get("bronze", "BronzeLH")
    silver_lh = lakehouses.get("silver", "SilverLH")
    gold_lh = lakehouses.get("gold", "GoldLH")
    pipeline_name = (
        artifacts.get("dataPipeline")
        or f"PL_{company}_Orchestration"
    )

    domains: list[str] = []
    if sample_data_config:
        domains = [
            d["name"]
            for d in sample_data_config.get("sampleData", {}).get("domains", [])
        ]
    if not domains:
        domains = industry.get("domains", ["Default"])

    # ── Layout: horizontal columns, evenly spaced vertically per domain ──────
    X = [80, 280, 490, 700, 910, 1120, 1330, 1540, 1750]
    domain_count = len(domains)
    mid_y = 80 + (domain_count - 1) * 65 // 2  # vertical centre

    nodes: list[dict] = []
    edges: list[dict] = []

    # Col 0 — CSV source files
    nodes.append(_node("csv-source", "Source", "Sample CSV Files",
                        f"{domain_count} domain(s) · raw CSV input",
                        X[0], mid_y))

    # Col 1 — Dataflow Gen2 per domain
    for i, domain in enumerate(domains):
        y = 80 + i * 65
        nid = f"df-{domain.lower()}"
        nodes.append(_node(nid, "Dataflow", f"DF_{domain}",
                            f"Power Query M → {bronze_lh}", X[1], y))
        edges.append(_edge(f"csv-to-{nid}", "csv-source", nid, "CSV files"))

    # Col 2 — Bronze Lakehouse
    nodes.append(_node("bronze-lh", "Lakehouse", bronze_lh,
                        "Raw ingestion layer", X[2], mid_y))
    for domain in domains:
        edges.append(_edge(f"df-{domain.lower()}-to-bronze",
                           f"df-{domain.lower()}", "bronze-lh", "Delta tables"))

    # Col 3 — NB01 Bronze→Silver + NB02 Web Enrichment
    nodes.append(_node("nb01", "Notebook", "NB01: Bronze→Silver",
                        "Cleaning · typing · dedup · null handling",
                        X[3], mid_y - 35))
    nodes.append(_node("nb02", "Notebook", "NB02: Web Enrichment",
                        "External API data → SilverLH.web schema",
                        X[3], mid_y + 35))
    edges.append(_edge("bronze-to-nb01", "bronze-lh", "nb01", "PySpark read"))

    # Col 4 — Silver Lakehouse
    nodes.append(_node("silver-lh", "Lakehouse", silver_lh,
                        "Cleaned · typed · domain schemas", X[4], mid_y))
    edges.append(_edge("nb01-to-silver", "nb01", "silver-lh", "Delta write"))
    edges.append(_edge("nb02-to-silver", "nb02", "silver-lh", "Delta write"))

    # Col 5 — NB03 Silver→Gold + Pipeline orchestrator
    nodes.append(_node("nb03", "Notebook", "NB03: Silver→Gold",
                        "Star schema · Dim/Fact · DimDate generation",
                        X[5], mid_y - 50))
    nodes.append(_node("pipeline", "Pipeline", pipeline_name,
                        "Orchestrates: Dataflows → NB01 → NB02 → NB03 → NB04 → NB05 → NB06",
                        X[5], mid_y + 80))
    edges.append(_edge("silver-to-nb03", "silver-lh", "nb03", "PySpark read"))

    # Col 6 — Gold Lakehouse
    nodes.append(_node("gold-lh", "Lakehouse", gold_lh,
                        "Analytics-ready star schema", X[6], mid_y))
    edges.append(_edge("nb03-to-gold", "nb03", "gold-lh", "Delta write"))

    # Col 7 — Semantic Model + optional NB04/NB05/NB07
    nodes.append(_node("semantic-model", "SemanticModel", f"{company}Model",
                        "Direct Lake · TMDL · DAX measures",
                        X[7], mid_y - 90))
    edges.append(_edge("gold-to-sm", "gold-lh", "semantic-model", "Direct Lake"))

    extra_y = mid_y + 70
    if not skip_forecast:
        nodes.append(_node("nb04", "Notebook", "NB04: Forecasting",
                            "Holt-Winters · MLflow tracking",
                            X[7], extra_y))
        edges.append(_edge("gold-to-nb04", "gold-lh", "nb04", "PySpark read"))
        extra_y += 70

    if not skip_htap:
        nodes.append(_node("nb05", "Notebook", "NB05: Event Simulator",
                            "Synthetic real-time event generation",
                            X[7], extra_y))
        nodes.append(_node("eventhouse", "Eventhouse",
                            f"RT_{company}_Events",
                            "KQL Database · hot-path analytics",
                            X[8], extra_y))
        edges.append(_edge("nb05-to-eventhouse",
                           "nb05", "eventhouse", "EventStream"))
        extra_y += 70

    if not skip_writeback:
        nodes.append(_node("nb07-09", "Notebook", "NB07–NB09: Writeback",
                            "User Data Function · SQL Database upserts",
                            X[7], extra_y))
        edges.append(_edge("gold-to-writeback",
                           "gold-lh", "nb07-09", "Delta read"))
        extra_y += 70

    # Col 8 — Reports + Data Agent
    report_types = ["Analytics", "Forecasting", "HTAP", "Pipeline"]
    if skip_htap:
        report_types.remove("HTAP")
    report_y = mid_y - 120
    for rtype in report_types:
        rid = f"report-{rtype.lower()}"
        nodes.append(_node(rid, "Report", f"{company}-{rtype}",
                            "PBIR v4.0 · Power BI report",
                            X[8], report_y))
        edges.append(_edge(f"sm-to-{rid}", "semantic-model", rid, "Power BI"))
        report_y += 55

    nodes.append(_node("data-agent", "DataAgent", f"{company}Agent",
                        "Fabric AI Q&A · natural language on semantic model",
                        X[8], mid_y - 200))
    edges.append(_edge("sm-to-agent", "semantic-model", "data-agent",
                       "Semantic Link"))

    # ── Assemble definition ───────────────────────────────────────────────────
    canvas_height = max(650, extra_y + 120)
    definition = {
        "schemaVersion": "1.0",
        "metadata": {
            "name": f"{industry['displayName']} — Data Architecture",
            "description": industry.get("description", ""),
            "industry": industry["id"],
            "generatedBy": "FabricEndtoEnd generator",
        },
        "layout": {
            "type": "HorizontalFlow",
            "direction": "left-to-right",
            "canvasWidth": 1900,
            "canvasHeight": canvas_height,
        },
        "nodes": nodes,
        "edges": edges,
    }

    taskflow_dir = output_dir / "TaskFlow"
    taskflow_dir.mkdir(parents=True, exist_ok=True)
    out_path = taskflow_dir / "taskflow-definition.json"
    out_path.write_text(
        json.dumps(definition, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return [out_path]


def _node(node_id: str, node_type: str, label: str,
          description: str, x: int, y: int) -> dict:
    return {
        "id": node_id,
        "type": node_type,
        "label": label,
        "description": description,
        "position": {"x": x, "y": y},
    }


def _edge(edge_id: str, source: str, target: str, label: str) -> dict:
    return {
        "id": edge_id,
        "source": source,
        "target": target,
        "label": label,
    }


# ─── Workspace Icon ───────────────────────────────────────────────────────────

def _pick_symbol(industry_id: str, domains: list[str]) -> str:
    """Choose the best icon symbol key for the given industry."""
    if industry_id in _INDUSTRY_SYMBOL_MAP:
        return _INDUSTRY_SYMBOL_MAP[industry_id]
    joined = " ".join(d.lower() for d in domains)
    if any(kw in joined for kw in ("energy", "power", "solar", "wind", "grid")):
        return "energy"
    if any(kw in joined for kw in ("book", "publish", "media", "content")):
        return "book"
    if any(kw in joined for kw in ("hr", "payroll", "finance", "accounting", "budget")):
        return "finance"
    if any(kw in joined for kw in ("production", "manufactur", "quality", "supply")):
        return "manufacturing"
    return "default"


def _initials(display_name: str) -> str:
    """Return up to 2 uppercase initials from a display name."""
    parts = display_name.replace("-", " ").split()
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()
    return display_name[:2].upper()


def _generate_workspace_icon(industry_config: dict, output_dir: Path) -> list[Path]:
    """Generate a 200×200 SVG icon branded for the industry."""
    industry = industry_config["industry"]
    theme = industry.get("theme", {})
    primary = theme.get("primary", "#1565C0")
    secondary = theme.get("secondary", "#E65100")
    industry_id = industry.get("id", "")
    domains = industry.get("domains", [])
    symbol_key = _pick_symbol(industry_id, domains)
    symbol_path_d = _ICON_PATHS[symbol_key]
    abbrev = _initials(industry.get("displayName", industry_id))
    display_name = industry.get("displayName", industry_id)

    svg = _build_icon_svg(primary, secondary, symbol_path_d, abbrev, display_name)

    icon_dir = output_dir / "WorkspaceIcon"
    icon_dir.mkdir(parents=True, exist_ok=True)
    icon_path = icon_dir / "icon.svg"
    icon_path.write_text(svg, encoding="utf-8")
    return [icon_path]


def _build_icon_svg(primary: str, secondary: str, symbol_path_d: str,
                    abbrev: str, display_name: str) -> str:
    """Build a 200×200 SVG workspace icon string."""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<svg xmlns="http://www.w3.org/2000/svg"
     viewBox="0 0 200 200" width="200" height="200">
  <title>{display_name}</title>
  <desc>Fabric workspace icon for {display_name}</desc>
  <defs>
    <linearGradient id="bg" x1="0%" y1="0%" x2="100%" y2="100%">
      <stop offset="0%"   stop-color="#ffffff" stop-opacity="0.15"/>
      <stop offset="100%" stop-color="#000000" stop-opacity="0.12"/>
    </linearGradient>
    <clipPath id="shape">
      <rect width="200" height="200" rx="36" ry="36"/>
    </clipPath>
  </defs>

  <!-- Background -->
  <rect width="200" height="200" rx="36" ry="36" fill="{primary}"/>
  <rect width="200" height="200" rx="36" ry="36" fill="url(#bg)"/>

  <!-- Symbol (24×24 design space scaled ×5, centred at 100, 88) -->
  <g clip-path="url(#shape)">
    <g transform="translate(100 88) scale(4.8) translate(-12 -12)"
       fill="white" opacity="0.93">
      <path d="{symbol_path_d}"/>
    </g>

    <!-- Accent strip -->
    <rect x="0" y="152" width="200" height="48" fill="{secondary}" opacity="0.88"/>

    <!-- Abbreviation label -->
    <text x="100" y="177"
          text-anchor="middle" dominant-baseline="middle"
          font-family="Segoe UI, Segoe, Arial, sans-serif"
          font-size="22" font-weight="700" fill="white" letter-spacing="4">
      {abbrev}
    </text>
  </g>
</svg>
"""
