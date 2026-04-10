"""Shortcut generator — produces Lakehouse shortcut definitions.

Generates from industry configs:
  Shortcuts/
    shortcuts.json          — Shortcut metadata for all 3 Lakehouses
    README.md               — Documentation on shortcut deployment

Shortcuts provide an alternative ingestion path that references external
data in OneLake, ADLS Gen2, or S3 without copying data into the Lakehouse.
"""

import json
from pathlib import Path


def generate_shortcuts(industry_config: dict,
                       sample_data_config: dict | None,
                       output_dir: Path) -> list[Path]:
    """Generate Lakehouse shortcut definitions.

    Args:
        industry_config: Parsed industry.json content.
        sample_data_config: Parsed sample-data.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    if not sample_data_config:
        return []

    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo")

    sd = sample_data_config.get("sampleData", {})
    domains = sd.get("domains", [])
    if not domains:
        return []

    shortcuts_dir = output_dir / "Shortcuts"
    shortcuts_dir.mkdir(parents=True, exist_ok=True)
    created = []

    # Build shortcut definitions per lakehouse layer
    bronze_shortcuts = []
    for domain in domains:
        folder = domain.get("folder", domain["name"].lower())
        for table in domain.get("tables", []):
            table_name = table.get("name", "")
            bronze_shortcuts.append({
                "name": table_name,
                "path": f"Tables/{table_name}",
                "target": {
                    "type": "oneLake",
                    "oneLake": {
                        "workspaceId": "{{SOURCE_WORKSPACE_ID}}",
                        "lakehouseId": "{{SOURCE_LAKEHOUSE_ID}}",
                        "path": f"Tables/{table_name}",
                    },
                },
                "metadata": {
                    "domain": domain["name"],
                    "folder": folder,
                    "description": table.get("description",
                                             f"{table_name} data from {domain['name']}"),
                },
            })

    # ADLS Gen2 alternative for Bronze
    adls_shortcuts = []
    for domain in domains:
        folder = domain.get("folder", domain["name"].lower())
        for table in domain.get("tables", []):
            table_name = table.get("name", "")
            adls_shortcuts.append({
                "name": table_name,
                "path": f"Files/{folder}/{table_name}.csv",
                "target": {
                    "type": "adlsGen2",
                    "adlsGen2": {
                        "connectionId": "{{ADLS_CONNECTION_ID}}",
                        "location": "https://{{STORAGE_ACCOUNT}}.dfs.core.windows.net",
                        "subpath": f"/{{CONTAINER}}/{folder}/{table_name}.csv",
                    },
                },
            })

    # S3 alternative
    s3_shortcuts = []
    for domain in domains:
        folder = domain.get("folder", domain["name"].lower())
        for table in domain.get("tables", []):
            table_name = table.get("name", "")
            s3_shortcuts.append({
                "name": table_name,
                "path": f"Files/{folder}/{table_name}.csv",
                "target": {
                    "type": "s3",
                    "s3": {
                        "connectionId": "{{S3_CONNECTION_ID}}",
                        "location": "https://{{S3_BUCKET}}.s3.{{AWS_REGION}}.amazonaws.com",
                        "subpath": f"/{folder}/{table_name}.csv",
                    },
                },
            })

    shortcuts_def = {
        "name": f"{company.replace(' ', '')}Shortcuts",
        "description": f"Lakehouse shortcut definitions for {company}",
        "lakehouses": {
            "BronzeLH": {
                "oneLakeShortcuts": bronze_shortcuts,
                "adlsGen2Shortcuts": adls_shortcuts,
                "s3Shortcuts": s3_shortcuts,
            },
        },
        "metadata": {
            "generatedBy": "FabricEndToEnd",
            "industry": industry.get("id", ""),
            "totalShortcuts": len(bronze_shortcuts),
            "supportedTargets": ["oneLake", "adlsGen2", "s3"],
        },
    }

    # Write shortcuts.json
    shortcuts_path = shortcuts_dir / "shortcuts.json"
    shortcuts_path.write_text(
        json.dumps(shortcuts_def, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    created.append(shortcuts_path)

    # Write README.md
    readme_lines = [
        f"# {company} Lakehouse Shortcuts",
        "",
        f"Alternative ingestion path using Fabric Lakehouse shortcuts instead of CSV upload.",
        "",
        "## Overview",
        "",
        f"This folder contains shortcut definitions for **{len(bronze_shortcuts)} tables** "
        f"across {len(domains)} domains.",
        "",
        "Shortcuts create a virtual reference to external data without copying it, enabling:",
        "- **Zero-copy ingestion** from OneLake, ADLS Gen2, or Amazon S3",
        "- **Real-time data access** without ETL delay",
        "- **Cost reduction** by avoiding data duplication",
        "",
        "## Supported Targets",
        "",
        "| Target | Template Variable | Description |",
        "|--------|------------------|-------------|",
        "| OneLake | `{{SOURCE_WORKSPACE_ID}}`, `{{SOURCE_LAKEHOUSE_ID}}` | Cross-workspace Lakehouse reference |",
        "| ADLS Gen2 | `{{ADLS_CONNECTION_ID}}`, `{{STORAGE_ACCOUNT}}`, `{{CONTAINER}}` | Azure Data Lake Storage Gen2 |",
        "| Amazon S3 | `{{S3_CONNECTION_ID}}`, `{{S3_BUCKET}}`, `{{AWS_REGION}}` | Amazon S3 bucket |",
        "",
        "## Shortcuts by Domain",
        "",
        "| Domain | Tables |",
        "|--------|--------|",
    ]

    for domain in domains:
        tables = [t["name"] for t in domain.get("tables", [])]
        readme_lines.append(f"| {domain['name']} | {', '.join(tables)} |")

    readme_lines.extend([
        "",
        "## Deployment",
        "",
        "```powershell",
        "# Create shortcuts via Fabric REST API",
        f'$shortcuts = Get-Content "Shortcuts/shortcuts.json" | ConvertFrom-Json',
        "foreach ($sc in $shortcuts.lakehouses.BronzeLH.oneLakeShortcuts) {",
        '    $body = @{ name = $sc.name; path = $sc.path; target = $sc.target } | ConvertTo-Json -Depth 10',
        '    Invoke-FabricApi -Method POST -Uri "lakehouses/$lhId/shortcuts" -Body $body',
        "}",
        "```",
        "",
        "Replace `{{PLACEHOLDER}}` values with actual connection details before deployment.",
        "",
    ])

    readme_path = shortcuts_dir / "README.md"
    readme_path.write_text("\n".join(readme_lines), encoding="utf-8")
    created.append(readme_path)

    return created
