"""Mirroring generator — produces Fabric Mirroring definitions.

Generates from industry configs:
  Mirroring/
    mirroring-definition.json — Mirroring item definition for external DB connectivity
    README.md                 — Documentation on mirroring setup

Fabric Mirroring replicates external databases (SQL Server, Azure SQL,
PostgreSQL, Cosmos DB, Snowflake) into OneLake Delta tables automatically.
"""

import json
from pathlib import Path


# Supported mirroring source types
_SOURCE_TYPES = {
    "sqlServer": {
        "displayName": "SQL Server",
        "connectionTemplate": {
            "type": "sqlServer",
            "server": "{{SQL_SERVER}}",
            "database": "{{SQL_DATABASE}}",
            "authType": "sql",
            "username": "{{SQL_USERNAME}}",
            "password": "{{SQL_PASSWORD}}",
        },
    },
    "azureSql": {
        "displayName": "Azure SQL Database",
        "connectionTemplate": {
            "type": "azureSql",
            "server": "{{AZURE_SQL_SERVER}}.database.windows.net",
            "database": "{{AZURE_SQL_DATABASE}}",
            "authType": "servicePrincipal",
        },
    },
    "cosmosDb": {
        "displayName": "Azure Cosmos DB (NoSQL)",
        "connectionTemplate": {
            "type": "cosmosDb",
            "accountEndpoint": "{{COSMOS_ENDPOINT}}",
            "database": "{{COSMOS_DATABASE}}",
            "authType": "key",
        },
    },
    "postgresql": {
        "displayName": "Azure Database for PostgreSQL",
        "connectionTemplate": {
            "type": "postgresql",
            "server": "{{PG_SERVER}}.postgres.database.azure.com",
            "database": "{{PG_DATABASE}}",
            "authType": "password",
        },
    },
    "snowflake": {
        "displayName": "Snowflake",
        "connectionTemplate": {
            "type": "snowflake",
            "account": "{{SNOWFLAKE_ACCOUNT}}",
            "warehouse": "{{SNOWFLAKE_WAREHOUSE}}",
            "database": "{{SNOWFLAKE_DATABASE}}",
        },
    },
}


def generate_mirroring(industry_config: dict,
                       sample_data_config: dict | None,
                       output_dir: Path) -> list[Path]:
    """Generate Fabric Mirroring definitions.

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

    mirror_dir = output_dir / "Mirroring"
    mirror_dir.mkdir(parents=True, exist_ok=True)
    created = []

    # Build table list for mirroring
    tables = []
    for domain in domains:
        schema = domain.get("folder", domain["name"].lower())
        for table in domain.get("tables", []):
            table_name = table.get("name", "")
            columns = table.get("columns", [])
            tables.append({
                "sourceSchema": schema,
                "sourceTable": table_name,
                "destinationTable": table_name,
                "columns": [
                    {
                        "name": c.get("name", ""),
                        "sourceType": c.get("type", "STRING"),
                    }
                    for c in columns[:10]  # Cap for readability
                ],
                "enabled": True,
            })

    # Build mirroring definition with templates for each source type
    mirroring_def = {
        "name": f"{company.replace(' ', '')}Mirror",
        "displayName": f"{company} Database Mirror",
        "description": f"Fabric Mirroring definition for {company} — "
                       f"replicates {len(tables)} tables into OneLake",
        "sourceConfigurations": {
            src_type: {
                "displayName": meta["displayName"],
                "connection": meta["connectionTemplate"],
                "tables": tables,
            }
            for src_type, meta in _SOURCE_TYPES.items()
        },
        "destination": {
            "type": "oneLake",
            "lakehouse": "BronzeLH",
            "format": "delta",
        },
        "replicationPolicy": {
            "mode": "continuous",
            "initialSnapshotMode": "full",
            "changeDataCapture": True,
        },
        "metadata": {
            "generatedBy": "FabricEndToEnd",
            "industry": industry.get("id", ""),
            "tableCount": len(tables),
        },
    }

    # Write definition
    def_path = mirror_dir / "mirroring-definition.json"
    def_path.write_text(
        json.dumps(mirroring_def, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    created.append(def_path)

    # Write README
    readme_lines = [
        f"# {company} Database Mirroring",
        "",
        f"Fabric Mirroring definitions for replicating external databases into OneLake.",
        "",
        "## Overview",
        "",
        f"This mirroring definition covers **{len(tables)} tables** across "
        f"{len(domains)} domains, with templates for 5 source types.",
        "",
        "## Supported Sources",
        "",
        "| Source | Template Variables |",
        "|--------|-------------------|",
    ]

    for src_type, meta in _SOURCE_TYPES.items():
        conn = meta["connectionTemplate"]
        placeholders = [v for v in conn.values() if isinstance(v, str) and v.startswith("{{")]
        readme_lines.append(f"| {meta['displayName']} | {', '.join(placeholders)} |")

    readme_lines.extend([
        "",
        "## Tables",
        "",
        "| # | Domain | Table |",
        "|---|--------|-------|",
    ])

    i = 0
    for domain in domains:
        for table in domain.get("tables", []):
            i += 1
            readme_lines.append(f"| {i} | {domain['name']} | {table['name']} |")

    readme_lines.extend([
        "",
        "## Deployment",
        "",
        "1. Choose a source type from `mirroring-definition.json`",
        "2. Replace `{{PLACEHOLDER}}` values with actual connection details",
        "3. Create the Mirrored Database item via Fabric REST API or portal",
        "",
        "```powershell",
        "# Create via Fabric REST API",
        f'$def = Get-Content "Mirroring/mirroring-definition.json" | ConvertFrom-Json',
        '# Select source: $def.sourceConfigurations.azureSql',
        '# POST to /v1/workspaces/$wsId/mirroredDatabases',
        "```",
        "",
        "## Replication Policy",
        "",
        "- **Mode**: Continuous (CDC-based real-time replication)",
        "- **Initial Snapshot**: Full table copy on first sync",
        "- **Change Data Capture**: Enabled — incremental updates after initial load",
        "",
    ])

    readme_path = mirror_dir / "README.md"
    readme_path.write_text("\n".join(readme_lines), encoding="utf-8")
    created.append(readme_path)

    return created
