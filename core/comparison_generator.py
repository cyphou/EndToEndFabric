"""Cross-industry comparison report generator.

Loads all industry configs and produces a Markdown comparison matrix
showing data volumes, model complexity, report coverage, and feature flags.
"""

import json
from pathlib import Path

from core.config_loader import list_industries, load_all_configs


def generate_comparison(output_dir: Path) -> Path:
    """Generate a Markdown comparison report across all industries.

    Args:
        output_dir: Directory to write the comparison report.

    Returns:
        Path to the generated report file.
    """
    industries = list_industries()
    rows: list[dict] = []

    for ind_id in industries:
        configs = load_all_configs(ind_id)
        rows.append(_collect_metrics(ind_id, configs))

    output_dir.mkdir(parents=True, exist_ok=True)
    report_path = output_dir / "Cross-Industry-Comparison.md"

    lines: list[str] = []
    lines.append("# Cross-Industry Comparison Report\n")
    lines.append("Auto-generated comparison of all configured industry demos.\n")

    # Data volume table
    lines.append("## Data Volume\n")
    lines.append("| Metric | " + " | ".join(r["label"] for r in rows) + " |")
    lines.append("|--------|" + "|".join("---:" for _ in rows) + "|")
    for metric, key in [
        ("CSV files", "csv_count"),
        ("Total CSV rows", "csv_rows"),
        ("Domains", "domain_count"),
    ]:
        lines.append(f"| {metric} | " + " | ".join(str(r[key]) for r in rows) + " |")

    # Semantic model table
    lines.append("\n## Semantic Model Complexity\n")
    lines.append("| Metric | " + " | ".join(r["label"] for r in rows) + " |")
    lines.append("|--------|" + "|".join("---:" for _ in rows) + "|")
    for metric, key in [
        ("Tables", "tables"),
        ("Measures", "measures"),
        ("Relationships", "relationships"),
        ("Calc columns", "calc_columns"),
    ]:
        lines.append(f"| {metric} | " + " | ".join(str(r[key]) for r in rows) + " |")

    # Report coverage table
    lines.append("\n## Report Coverage\n")
    lines.append("| Metric | " + " | ".join(r["label"] for r in rows) + " |")
    lines.append("|--------|" + "|".join("---:" for _ in rows) + "|")
    for metric, key in [
        ("Reports", "report_count"),
        ("Total pages", "total_pages"),
        ("Analytics pages", "analytics_pages"),
        ("Forecast pages", "forecast_pages"),
        ("HTAP pages", "htap_pages"),
    ]:
        lines.append(f"| {metric} | " + " | ".join(str(r[key]) for r in rows) + " |")

    # Feature flags table
    lines.append("\n## Feature Flags\n")
    lines.append("| Feature | " + " | ".join(r["label"] for r in rows) + " |")
    lines.append("|---------|" + "|".join("---:" for _ in rows) + "|")
    for metric, key in [
        ("Forecast", "has_forecast"),
        ("HTAP", "has_htap"),
        ("Writeback", "has_writeback"),
        ("Data Agent", "has_agent"),
        ("Web Enrichment", "has_web_enrichment"),
    ]:
        lines.append(f"| {metric} | " + " | ".join(r[key] for r in rows) + " |")

    # HTAP detail
    lines.append("\n## HTAP Detail\n")
    lines.append("| Metric | " + " | ".join(r["label"] for r in rows) + " |")
    lines.append("|--------|" + "|".join("---:" for _ in rows) + "|")
    for metric, key in [
        ("Event streams", "htap_streams"),
        ("KQL tables", "htap_kql_tables"),
    ]:
        lines.append(f"| {metric} | " + " | ".join(str(r[key]) for r in rows) + " |")

    # Forecast detail
    lines.append("\n## Forecast Detail\n")
    lines.append("| Metric | " + " | ".join(r["label"] for r in rows) + " |")
    lines.append("|--------|" + "|".join("---:" for _ in rows) + "|")
    for metric, key in [
        ("Forecast models", "forecast_models"),
        ("Planning models", "planning_models"),
    ]:
        lines.append(f"| {metric} | " + " | ".join(str(r[key]) for r in rows) + " |")

    lines.append("")
    report_path.write_text("\n".join(lines), encoding="utf-8")
    return report_path


def _collect_metrics(industry_id: str, configs: dict) -> dict:
    """Extract comparison metrics from an industry's config set."""
    ind = configs.get("industry", {}).get("industry", {})
    label = ind.get("name", industry_id)

    # Sample data metrics — structure: {"sampleData": {"domains": [{"tables": [...]}]}}
    sd = configs.get("sample_data", {}) or {}
    sd_inner = sd.get("sampleData", sd)
    domains = sd_inner.get("domains", [])
    domain_count = len(domains)
    tables_flat = [t for d in domains for t in d.get("tables", [])]
    csv_count = len(tables_flat)
    csv_rows = sum(t.get("rowCount", t.get("row_count", 0)) for t in tables_flat)

    # Semantic model — structure: {"semanticModel": {"tables": [...], "relationships": [...]}}
    sm = configs.get("semantic_model", {}) or {}
    sm_inner = sm.get("semanticModel", sm)
    sm_tables = sm_inner.get("tables", [])
    tbl_count = len(sm_tables)
    # Measures can be on tables or at top level
    measure_count = len(sm_inner.get("measures", []))
    measure_count += sum(len(t.get("measures", [])) for t in sm_tables)
    rel_count = len(sm_inner.get("relationships", []))
    calc_col_count = sum(
        len([c for c in t.get("columns", []) if c.get("expression")])
        for t in sm_tables
    )

    # Report metrics — structure: {"reports": [...]}
    rpt = configs.get("reports", {}) or {}
    reports = rpt.get("reports", [])
    report_count = len(reports)
    total_pages = sum(len(r.get("pages", [])) for r in reports)
    analytics_pages = _pages_for_type(reports, "Analytics")
    forecast_pages = _pages_for_type(reports, "Forecasting")
    htap_pages = _pages_for_type(reports, "HTAP")

    # Feature flags
    has_forecast = "Yes" if configs.get("forecast") else "No"
    has_htap = "Yes" if configs.get("htap") else "No"
    has_writeback = "Yes" if configs.get("writeback") else "No"
    has_agent = "Yes" if configs.get("data_agent") else "No"
    has_web = "Yes" if configs.get("web_enrichment") else "No"

    # HTAP detail — multiple structures possible
    htap_cfg = configs.get("htap", {}) or {}
    htap_inner = htap_cfg.get("htapConfig", htap_cfg)
    htap_streams = len(htap_inner.get("eventStreams", htap_inner.get("event_streams", [])))
    eh = htap_inner.get("eventhouse", htap_inner.get("kqlDatabase", {}))
    htap_kql = len(eh.get("tables", []))

    # Forecast detail — multiple structures possible
    fc_cfg = configs.get("forecast", {}) or {}
    fc_inner = fc_cfg.get("forecastConfig", fc_cfg)
    fc_models = len(fc_inner.get("forecastModels", fc_inner.get("models", [])))
    pl_cfg = configs.get("planning", {}) or {}
    pl_inner = pl_cfg.get("planningConfig", pl_cfg.get("planning", pl_cfg))
    pl_models = len(pl_inner.get("planningModels", pl_inner.get("models", [])))

    return {
        "id": industry_id,
        "label": label,
        "csv_count": csv_count,
        "csv_rows": csv_rows,
        "domain_count": domain_count,
        "tables": tbl_count,
        "measures": measure_count,
        "relationships": rel_count,
        "calc_columns": calc_col_count,
        "report_count": report_count,
        "total_pages": total_pages,
        "analytics_pages": analytics_pages,
        "forecast_pages": forecast_pages,
        "htap_pages": htap_pages,
        "has_forecast": has_forecast,
        "has_htap": has_htap,
        "has_writeback": has_writeback,
        "has_agent": has_agent,
        "has_web_enrichment": has_web,
        "htap_streams": htap_streams,
        "htap_kql_tables": htap_kql,
        "forecast_models": fc_models,
        "planning_models": pl_models,
    }


def _pages_for_type(reports: list[dict], report_type: str) -> int:
    """Count pages in reports whose name contains the given type string."""
    count = 0
    for r in reports:
        if report_type.lower() in r.get("name", "").lower():
            count += len(r.get("pages", []))
    return count
