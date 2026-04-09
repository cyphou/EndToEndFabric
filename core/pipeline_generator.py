"""Pipeline generator -- produces Fabric Data Pipeline JSON definitions.

Generates a pipeline-content.json with activities orchestrating:
Phase 1: NB01 (ingests CSVs + Bronze -> Silver)
Phase 2-N: Sequential notebook activities (Silver->Gold->Forecast)
"""

import json
from pathlib import Path


def generate_pipeline(industry_config: dict, sample_data_config: dict | None,
                      output_dir: Path,
                      skip_forecast: bool = False,
                      skip_htap: bool = False) -> list[Path]:
    """Generate pipeline definition JSON for the industry demo.

    Returns list of created file paths.
    """
    industry = industry_config["industry"]
    company_prefix = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    domains = []
    if sample_data_config:
        domains = [d["name"] for d in sample_data_config.get("sampleData", {}).get("domains", [])]

    pipeline_dir = output_dir / "Pipeline"
    pipeline_dir.mkdir(parents=True, exist_ok=True)

    activities = []

    # Sequential notebooks -- NB01 ingests CSVs itself, subsequent notebooks depend on prior
    # (RefreshDataflow activities removed; NB01 now self-contained for CSV ingestion)
    notebook_sequence = [
        ("01", "BronzeToSilver", "2. Transformation",
         "PySpark: BronzeLH tables → SilverLH Delta. Quality checks, transforms, dedup."),
        ("02", "WebEnrichment", "2. Transformation",
         "PySpark: fetches external API data → SilverLH.web schema."),
        ("03", "SilverToGold", "3. Gold Layer",
         "PySpark: SilverLH → GoldLH star schema (dim/fact schemas) + analytics views."),
    ]

    if not skip_forecast:
        notebook_sequence.append(
            ("04", "Forecasting", "4. Forecasting",
             "PySpark: Builds Holt-Winters forecasts on Gold data."))

    if not skip_htap:
        notebook_sequence.append(
            ("05", "EventSimulator", "5. HTAP",
             "PySpark: Simulates real-time events for Eventhouse ingestion."))

    # NB06 Diagnostic always runs last
    notebook_sequence.append(
        ("06", "DiagnosticCheck", "6. Validation",
         "PySpark: Validates table row counts, schema integrity, and logs summary."))

    prev_activity = None
    for nb_num, nb_name, folder, description in notebook_sequence:
        act_name = f"{company_prefix}_{nb_num}_{nb_name}"
        if prev_activity:
            depends = [{"activity": prev_activity, "dependencyConditions": ["Succeeded"]}]
        else:
            depends = []

        activity = {
            "name": act_name,
            "type": "TridentNotebook",
            "description": description,
            "dependsOn": depends,
            "policy": {
                "timeout": "0.02:00:00" if nb_name == "SilverToGold" else "0.01:00:00",
                "retry": 1,
                "retryIntervalInSeconds": 60,
            },
            "typeProperties": {
                "notebookId": "{{" + f"NB{nb_num}_ID" + "}}",
                "workspaceId": "{{WORKSPACE_ID}}",
            },
            "folder": {"name": folder},
        }
        activities.append(activity)
        prev_activity = act_name

    pipeline_name = f"PL_{company_prefix}_Orchestration"
    nb_desc = " → ".join(f"NB{nb[0]}({nb[1]})" for nb in notebook_sequence)
    pipeline_content = {
        "properties": {
            "description": f"Medallion ETL: DF(CSV→Bronze) → {nb_desc}",
            "activities": activities,
        }
    }

    pipeline_path = pipeline_dir / "pipeline-content.json"
    pipeline_path.write_text(
        json.dumps(pipeline_content, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    # Write README
    readme_path = pipeline_dir / "README.md"
    lines = [
        f"# {pipeline_name}",
        "",
        "## Execution Flow",
        "",
    ]
    lines.append("```")
    lines.append("Phase 1 (parallel): " + " + ".join(f"DF_{d}" for d in domains))
    lines.append("    ↓ all succeed")
    for nb_num, nb_name, _, _ in notebook_sequence:
        lines.append(f"Phase {int(nb_num)+1}: NB{nb_num} {nb_name}")
        if nb_num != notebook_sequence[-1][0]:
            lines.append("    ↓")
    lines.append("```")
    lines.append("")
    lines.append(f"**Total activities:** {len(activities)}")
    lines.append("")
    lines.append("## Token Placeholders")
    lines.append("")
    lines.append("| Token | Description |")
    lines.append("|-------|-------------|")
    lines.append("| `{{WORKSPACE_ID}}` | Target Fabric workspace |")
    for domain in domains:
        lines.append(f"| `{{{{DF_{domain.upper()}_ID}}}}` | Dataflow ID for {domain} |")
    for nb_num, nb_name, _, _ in notebook_sequence:
        lines.append(f"| `{{{{NB{nb_num}_ID}}}}` | Notebook ID for {nb_name} |")

    readme_path.write_text("\n".join(lines), encoding="utf-8")

    return [pipeline_path, readme_path]
