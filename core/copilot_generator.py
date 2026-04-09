"""Copilot instructions generator — produces workspace-specific Copilot context.

Generates from industry configs:
  .copilot/instructions.md — Domain-specific semantic context for Copilot
"""

from pathlib import Path


def generate_copilot_instructions(industry_config: dict,
                                  semantic_model_config: dict | None,
                                  sample_data_config: dict | None,
                                  output_dir: Path) -> list[Path]:
    """Generate .copilot/instructions.md for the industry demo.

    Args:
        industry_config: Parsed industry.json content.
        semantic_model_config: Parsed semantic-model.json content.
        sample_data_config: Parsed sample-data.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo")
    display = industry.get("displayName", company)
    description = industry.get("description", "")
    domains = industry.get("domains", [])
    theme = industry.get("theme", {})

    copilot_dir = output_dir / ".copilot"
    copilot_dir.mkdir(parents=True, exist_ok=True)
    created = []

    lines = [
        f"# {display} — Copilot Context",
        "",
        f"> {description}",
        "",
        "## Domain Overview",
        "",
        f"This workspace contains a **Microsoft Fabric end-to-end demo** for **{display}**.",
        f"Domains: {', '.join(domains)}.",
        "",
        "## Architecture",
        "",
        "- **Medallion Lakehouse**: Bronze (raw CSV) → Silver (cleaned, typed) → Gold (star schema)",
        "- **Semantic Model**: Direct Lake on Gold Lakehouse (TMDL-defined)",
        "- **Reports**: PBIR v4.0 with 5 reports (Analytics, Forecasting, HTAP, Pipeline, Writeback)",
        "- **Notebooks**: PySpark ETL (NB01–NB09)",
        "- **Pipeline**: Fabric Data Pipeline orchestrating Dataflows → Notebooks",
        "",
    ]

    # Semantic model context
    sm = (semantic_model_config or {}).get("semanticModel", {})
    tables = sm.get("tables", [])
    measures = sm.get("measures", [])
    relationships = sm.get("relationships", [])

    if tables:
        lines.append("## Semantic Model")
        lines.append("")
        lines.append(f"Model: **{sm.get('name', company + 'Model')}** ({len(tables)} tables, "
                      f"{len(measures)} measures, {len(relationships)} relationships)")
        lines.append("")

        # Group tables by schema
        dim_tables = [t for t in tables if t.get("schema") == "dim" or t["name"].startswith("Dim")]
        fact_tables = [t for t in tables if t.get("schema") == "fact" or t["name"].startswith("Fact")]

        if dim_tables:
            lines.append("### Dimension Tables")
            lines.append("")
            for t in dim_tables:
                cols = [c["name"] for c in t.get("columns", [])]
                lines.append(f"- **{t['name']}**: {', '.join(cols[:6])}"
                             + (f" (+{len(cols)-6} more)" if len(cols) > 6 else ""))
            lines.append("")

        if fact_tables:
            lines.append("### Fact Tables")
            lines.append("")
            for t in fact_tables:
                cols = [c["name"] for c in t.get("columns", [])]
                lines.append(f"- **{t['name']}**: {', '.join(cols[:6])}"
                             + (f" (+{len(cols)-6} more)" if len(cols) > 6 else ""))
            lines.append("")

        # Key measures
        if measures:
            lines.append("### Key Measures (DAX)")
            lines.append("")
            for m in measures[:20]:
                lines.append(f"- `{m['name']}` = `{m['expression']}`")
            if len(measures) > 20:
                lines.append(f"- ... and {len(measures) - 20} more measures")
            lines.append("")

    # Sample data context
    sd = (sample_data_config or {}).get("sampleData", {})
    sd_domains = sd.get("domains", [])
    if sd_domains:
        lines.append("## Data Domains")
        lines.append("")
        for domain in sd_domains:
            table_names = [t["name"] for t in domain.get("tables", [])]
            lines.append(f"- **{domain['name']}**: {', '.join(table_names)}")
        lines.append("")

    # Theme context
    if theme:
        lines.append("## Theme")
        lines.append("")
        lines.append(f"- Primary: `{theme.get('primary', '#1565C0')}`")
        lines.append(f"- Secondary: `{theme.get('secondary', '#E65100')}`")
        lines.append(f"- Background: `{theme.get('background', '#FFFFFF')}`")
        lines.append("")

    # Naming conventions
    artifacts = industry_config.get("fabricArtifacts", {})
    lines.extend([
        "## Naming Conventions",
        "",
        f"- Lakehouses: BronzeLH, SilverLH, GoldLH",
        f"- Notebooks: NB01_BronzeToSilver through NB09_SQLDatabaseSetup",
        f"- Pipeline: {artifacts.get('dataPipeline', 'PL_' + company + '_Orchestration')}",
        f"- Reports: {company}-Analytics, {company}-Forecasting, {company}-HTAP, {company}-Pipeline, {company}-Writeback",
        f"- Semantic Model: {company}Model (DirectLake) + {company}WritebackModel (DirectQuery)",
        "",
    ])

    content = "\n".join(lines)
    out_path = copilot_dir / "instructions.md"
    out_path.write_text(content, encoding="utf-8")
    created.append(out_path)

    return created
