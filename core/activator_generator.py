"""Data Activator generator — produces Reflex trigger definitions.

Generates from htap-config.json alert thresholds:
  DataActivator/reflex-definition.json — Reflex item definition
  DataActivator/README.md              — Documentation
"""

import json
from pathlib import Path


def generate_data_activator(industry_config: dict,
                            htap_config: dict | None,
                            output_dir: Path) -> list[Path]:
    """Generate Data Activator Reflex trigger definitions.

    Args:
        industry_config: Parsed industry.json content.
        htap_config: Parsed htap-config.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    if not htap_config:
        return []

    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo")

    # Extract event streams from either format
    streams = (htap_config.get("htapConfig", {}).get("eventStreams", [])
               or htap_config.get("eventStreams", []))
    if not streams:
        return []

    da_dir = output_dir / "DataActivator"
    da_dir.mkdir(parents=True, exist_ok=True)
    created = []

    # Build triggers from event streams
    triggers = []
    for stream in streams:
        stream_name = stream.get("name", "Events")
        columns = stream.get("columns", stream.get("schema", []))

        # Find numeric columns for threshold triggers
        numeric_cols = [c for c in columns
                        if c.get("type", "") in ("float", "real", "int", "double", "decimal")]

        for col in numeric_cols[:2]:  # Max 2 triggers per stream
            col_name = col.get("name", "Value")
            triggers.append({
                "name": f"{stream_name}_{col_name}_Alert",
                "displayName": f"{stream_name} — {col_name} Threshold Alert",
                "description": f"Triggers when {col_name} in {stream_name} exceeds expected range",
                "source": {
                    "type": "EventStream",
                    "eventStream": stream_name,
                    "kqlTable": stream.get("kqlTable", stream_name),
                },
                "condition": {
                    "type": "threshold",
                    "column": col_name,
                    "operator": "greaterThan",
                    "value": 0,
                    "windowSeconds": 60,
                    "aggregation": "average",
                },
                "action": {
                    "type": "email",
                    "subject": f"[{company}] {stream_name} Alert: {col_name} threshold exceeded",
                    "recipients": ["{{ALERT_RECIPIENTS}}"],
                },
            })

    # Reflex definition
    reflex = {
        "name": f"{company.replace(' ', '')}Reflex",
        "displayName": f"{company} Data Activator",
        "description": f"Real-time alert triggers for {company} HTAP event streams",
        "triggers": triggers,
        "metadata": {
            "generatedBy": "FabricEndToEnd",
            "industry": industry.get("id", ""),
            "triggerCount": len(triggers),
        },
    }

    reflex_path = da_dir / "reflex-definition.json"
    reflex_path.write_text(
        json.dumps(reflex, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    created.append(reflex_path)

    # README
    readme_lines = [
        f"# {company} Data Activator",
        "",
        f"Real-time alert triggers for {company} HTAP event streams.",
        "",
        "## Triggers",
        "",
        f"| # | Trigger | Source | Condition |",
        f"|---|---------|--------|-----------|",
    ]
    for i, t in enumerate(triggers, 1):
        readme_lines.append(
            f"| {i} | {t['displayName']} | {t['source']['eventStream']} | "
            f"{t['condition']['column']} {t['condition']['operator']} threshold |"
        )
    readme_lines.extend([
        "",
        "## Deployment",
        "",
        "Data Activator (Reflex) requires a Fabric capacity (F64+).",
        "Deploy the `reflex-definition.json` via the Fabric REST API or import manually.",
        "",
        "## Configuration",
        "",
        "Update `{{ALERT_RECIPIENTS}}` in trigger actions with actual email addresses.",
        "",
    ])

    readme_path = da_dir / "README.md"
    readme_path.write_text("\n".join(readme_lines), encoding="utf-8")
    created.append(readme_path)

    return created
