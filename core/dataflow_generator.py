"""Dataflow Gen2 generator.

Generates Dataflow Gen2 Power Query M definitions for ingesting
CSV files from Bronze Lakehouse into typed tables.
"""

import json
from pathlib import Path


def generate_dataflows(industry_config: dict, sample_data_config: dict,
                       output_dir: Path) -> list[Path]:
    """Generate Dataflow Gen2 configuration files.

    Each domain in sample-data.json gets one Dataflow that ingests
    its CSV files into BronzeLH tables.

    Args:
        industry_config: Parsed industry.json content.
        sample_data_config: Parsed sample-data.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated Dataflow config file paths.
    """
    dataflows_dir = output_dir / "Dataflows"
    dataflows_dir.mkdir(parents=True, exist_ok=True)

    artifacts = industry_config.get("fabricArtifacts", {})
    bronze_lh = artifacts.get("lakehouses", {}).get("bronze", "BronzeLH")
    domains = sample_data_config.get("sampleData", {}).get("domains", [])

    generated = []

    for domain in domains:
        domain_name = domain.get("name", "Unknown")
        df_name = f"DF_{domain_name.replace(' ', '')}"
        tables = domain.get("tables", [])

        # Generate Power Query M queries for each table
        queries = []
        for table in tables:
            query = _generate_m_query(table, bronze_lh, domain.get("folder", domain_name))
            queries.append({
                "name": table["name"],
                "fileName": table.get("fileName", f"{table['name']}.csv"),
                "mQuery": query,
                "destinationTable": table["name"],
                "destinationLakehouse": bronze_lh,
            })

        # Write dataflow config
        df_config = {
            "dataflow": {
                "name": df_name,
                "domain": domain_name,
                "description": f"Ingests {len(tables)} CSV files from {domain_name} domain into {bronze_lh}",
                "destinationLakehouse": bronze_lh,
                "queries": queries,
            }
        }

        config_path = dataflows_dir / f"{df_name}.json"
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(df_config, f, indent=2, ensure_ascii=False)
        generated.append(config_path)

    # Generate summary README
    _write_dataflow_readme(dataflows_dir, domains, bronze_lh)
    generated.append(dataflows_dir / "README.md")

    return generated


def _generate_m_query(table: dict, lakehouse: str, folder: str) -> str:
    """Generate a Power Query M expression for a CSV table."""
    table_name = table["name"]
    file_name = table.get("fileName", f"{table_name}.csv")
    columns = table.get("columns", [])

    # Build type mapping
    type_map = []
    for col in columns:
        col_name = col["name"]
        m_type = _python_type_to_m_type(col.get("type", "string"))
        type_map.append('        {{"{}", {}}}'.format(col_name, m_type))

    type_list = ",\n".join(type_map)

    query = f'''let
    Source = Lakehouse.Contents(null){{[workspaceId=""]}}[Data],
    {lakehouse}_Data = Source{{[lakehouseId=""]}}[Data],
    Files = {lakehouse}_Data{{[Id="Files"]}}[Data],
    FolderData = Files{{[Name="{folder}"]}}[Data],
    CsvFile = FolderData{{[Name="{file_name}"]}}[Content],
    ParsedCsv = Csv.Document(CsvFile, [Delimiter=",", Encoding=65001, QuoteStyle=QuoteStyle.Csv]),
    PromotedHeaders = Table.PromoteHeaders(ParsedCsv, [PromoteAllScalars=true]),
    TypedColumns = Table.TransformColumnTypes(PromotedHeaders, {{
{type_list}
    }})
in
    TypedColumns'''

    return query


def _python_type_to_m_type(col_type: str) -> str:
    """Convert sample-data column type to Power Query M type."""
    type_mapping = {
        "string":   "type text",
        "int":      "Int64.Type",
        "float":    "type number",
        "decimal":  "Currency.Type",
        "date":     "type date",
        "datetime": "type datetime",
        "boolean":  "type logical",
    }
    return type_mapping.get(col_type, "type text")


def _write_dataflow_readme(dataflows_dir: Path, domains: list, bronze_lh: str):
    """Write a summary README for the Dataflows folder."""
    lines = [
        "# Dataflows Gen2\n\n",
        "Generated Dataflow configurations for CSV ingestion.\n\n",
        f"**Destination Lakehouse:** `{bronze_lh}`\n\n",
        "| Dataflow | Domain | Tables | CSV Files |\n",
        "|----------|--------|--------|-----------|\n",
    ]

    for domain in domains:
        name = domain.get("name", "Unknown")
        tables = domain.get("tables", [])
        files = ", ".join(t.get("fileName", "") for t in tables)
        lines.append(f"| DF_{name.replace(' ', '')} | {name} | {len(tables)} | {files} |\n")

    with open(dataflows_dir / "README.md", "w", encoding="utf-8") as f:
        f.writelines(lines)
