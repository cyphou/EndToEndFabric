"""HTAP generator — produces Eventhouse, KQL database, and event simulator artifacts.

Generates:
  - HTAP config schema + validation
  - Eventhouse definition JSON
  - KQL database definition with tables and functions
  - NB05 Event Simulator PySpark notebook
  - HTAP report page definitions
"""

import json
from pathlib import Path


def generate_htap(industry_config: dict, htap_config: dict,
                  output_dir: Path) -> list[Path]:
    """Generate HTAP / Transactional Analytics artifacts."""
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    gold_lh = artifacts.get("lakehouses", {}).get("gold", "GoldLH")

    hc = htap_config.get("htapConfig", htap_config)
    streams = hc.get("eventStreams", [])
    kql_db = hc.get("kqlDatabase", {})

    created = []

    # Write HTAP config
    htap_dir = output_dir / "Transactional"
    htap_dir.mkdir(parents=True, exist_ok=True)

    config_path = htap_dir / "htap-config.json"
    config_path.write_text(
        json.dumps(htap_config, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    created.append(config_path)

    # Eventhouse definition
    eh_path = htap_dir / "eventhouse-definition.json"
    eh_def = _build_eventhouse_definition(company, kql_db, streams)
    eh_path.write_text(json.dumps(eh_def, indent=2), encoding="utf-8")
    created.append(eh_path)

    # KQL database definition with table commands
    kql_path = htap_dir / "kql-database.kql"
    kql_code = _build_kql_database(company, kql_db, streams)
    kql_path.write_text(kql_code, encoding="utf-8")
    created.append(kql_path)

    # NB05 Event Simulator notebook
    nb_path = output_dir / "notebooks" / "05_EventSimulator.py"
    nb_path.parent.mkdir(parents=True, exist_ok=True)
    nb_code = _build_event_simulator(company, gold_lh, streams)
    nb_path.write_text(nb_code, encoding="utf-8")
    created.append(nb_path)

    # HTAP bridge notebook (KQL ↔ Lakehouse)
    bridge_path = htap_dir / "HotColdBridge.kql"
    bridge_code = _build_bridge_queries(company, gold_lh, kql_db, streams)
    bridge_path.write_text(bridge_code, encoding="utf-8")
    created.append(bridge_path)

    # README
    readme = _build_htap_readme(company, streams, kql_db)
    readme_path = htap_dir / "README.md"
    readme_path.write_text(readme, encoding="utf-8")
    created.append(readme_path)

    return created


def _build_eventhouse_definition(company, kql_db, streams):
    """Build Eventhouse definition JSON."""
    db_name = kql_db.get("name", f"{company}KQLDB")
    return {
        "displayName": f"{company}_Eventhouse",
        "description": f"Real-time event ingestion for {company} transactional analytics",
        "databases": [
            {
                "displayName": db_name,
                "description": kql_db.get("description", "HTAP hot-path query layer"),
                "retentionDays": kql_db.get("retentionDays", 365),
                "tables": [
                    {
                        "name": s["kqlTable"],
                        "description": s.get("description", ""),
                        "retentionDays": s.get("retentionDays", 90),
                    }
                    for s in streams
                ],
            }
        ],
    }


def _build_kql_database(company, kql_db, streams):
    """Build KQL database creation script."""
    lines = [
        f"// {company} — KQL Database Creation Script",
        f"// Database: {kql_db.get('name', company + 'KQLDB')}",
        "",
    ]

    for stream in streams:
        table = stream["kqlTable"]
        columns = stream.get("columns", [])
        col_defs = []
        for col in columns:
            kql_type = _map_kql_type(col.get("type", "string"))
            col_defs.append(f"    {col['name']}: {kql_type}")

        lines.append(f"// ── {table} ──")
        lines.append(f".create-merge table {table} (")
        lines.append(",\n".join(col_defs))
        lines.append(")")
        lines.append("")

        # Ingestion mapping
        mapping_cols = []
        for i, col in enumerate(columns):
            mapping_cols.append(
                f'    @\'{{\"column\":\"{col["name"]}\", '
                f'\"datatype\":\"{_map_kql_type(col.get("type", "string"))}\", '
                f'\"ordinal\":{i}}}\''
            )
        lines.append(f".create-or-alter table {table} ingestion json mapping '{table}_mapping'")
        lines.append("'['")
        lines.append(",\n".join(mapping_cols))
        lines.append("']'")
        lines.append("")

        # Retention policy
        retention = stream.get("retentionDays", 90)
        lines.append(f".alter-merge table {table} policy retention softdelete = {retention}d recoverability = enabled")
        lines.append("")

    return "\n".join(lines)


def _build_event_simulator(company, gold_lh, streams):
    """Build NB05 Event Simulator PySpark notebook."""
    stream_blocks = []
    for stream in streams:
        name = stream["name"]
        table = stream["kqlTable"]
        rate = stream.get("eventsPerSecond", 10)
        columns = stream.get("columns", [])

        col_assignments = []
        for col in columns:
            ctype = col.get("type", "string")
            cname = col["name"]
            if ctype == "datetime":
                col_assignments.append(f'            "{cname}": datetime.datetime.now().isoformat()')
            elif ctype == "float":
                col_assignments.append(f'            "{cname}": round(random.uniform(0, 1000), 2)')
            elif ctype == "int":
                col_assignments.append(f'            "{cname}": random.randint(1, 10000)')
            else:
                col_assignments.append(f'            "{cname}": f"{{random.choice(sample_ids)}}"')

        assignments_str = ",\n".join(col_assignments)

        stream_blocks.append(f'''
    # ── {name} ──
    print(f"  Simulating {name} ({rate} events/batch)...")
    events = []
    for _ in range({rate}):
        events.append({{
{assignments_str}
        }})
    df = spark.createDataFrame(events)
    df.write.mode("append").format("delta") \\
        .saveAsTable(f"{gold_lh}.analytics.{table}_staging")
    print(f"    ✓ {{len(events)}} events → {table}_staging")
''')

    all_blocks = "\n".join(stream_blocks)

    return f'''# Fabric Notebook
# {company} — NB05: Event Simulator
# Generates synthetic real-time events for HTAP scenarios.
# In production, replace with Event Stream or Eventhouse ingestion.

# CELL 1 — Setup
import random
import datetime
from pyspark.sql import Row

GOLD_LH = "{gold_lh}"
sample_ids = [f"ID-{{i:04d}}" for i in range(1, 101)]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_lh}.analytics")

print("=" * 60)
print(f"  {company} Event Simulator")
print("=" * 60)

# CELL 2 — Generate events
{all_blocks}

# CELL 3 — Summary
print("\\nEvent simulation complete")
'''


def _build_bridge_queries(company, gold_lh, kql_db, streams):
    """Build KQL queries that bridge hot (KQL) and cold (Lakehouse) paths."""
    db_name = kql_db.get("name", f"{company}KQLDB")
    lines = [
        f"// {company} — Hot-Cold Bridge Queries",
        f"// KQL Database: {db_name}",
        f"// Lakehouse: {gold_lh}",
        "",
        "// ── Union hot + cold data for real-time dashboards ──",
        "",
    ]

    for stream in streams:
        table = stream["kqlTable"]
        lines.append(f"// {stream['name']}: last 24h from KQL + historical from Lakehouse")
        lines.append(f"let HotData = {table} | where Timestamp > ago(24h);")
        lines.append(f"// let ColdData = external_table('{gold_lh}_analytics_{table}');")
        lines.append(f"// union HotData, ColdData")
        lines.append(f"HotData | summarize count() by bin(Timestamp, 1h)")
        lines.append("")

    return "\n".join(lines)


def _build_htap_readme(company, streams, kql_db):
    """Build README for HTAP artifacts."""
    stream_rows = []
    for s in streams:
        rate = s.get("eventsPerSecond", 10)
        stream_rows.append(f"| {s['name']} | {s['kqlTable']} | {rate}/s | {s.get('description', '')} |")

    return f"""# {company} — Transactional Analytics (HTAP)

## Architecture

```
Event Sources → Eventhouse → KQL Database → Real-time Dashboard
                    ↕ (Hot-Cold Bridge)
              Lakehouse (Gold) → Semantic Model → Analytics Reports
```

## Event Streams

| Stream | KQL Table | Rate | Description |
|--------|-----------|------|-------------|
{chr(10).join(stream_rows)}

## KQL Database: {kql_db.get('name', company + 'KQLDB')}

Retention: {kql_db.get('retentionDays', 365)} days

## Files

| File | Description |
|------|-------------|
| `htap-config.json` | HTAP configuration |
| `eventhouse-definition.json` | Eventhouse item definition |
| `kql-database.kql` | KQL table creation script |
| `HotColdBridge.kql` | Hot-cold bridge queries |
"""


def _map_kql_type(python_type: str) -> str:
    """Map column type to KQL data type."""
    return {
        "string": "string",
        "int": "int",
        "float": "real",
        "decimal": "decimal",
        "date": "datetime",
        "datetime": "datetime",
        "boolean": "bool",
        "guid": "guid",
    }.get(python_type, "string")
