"""TMDL Semantic Model generator.

Generates Tabular Model Definition Language (TMDL) files for a
Direct Lake semantic model from semantic-model.json config.
Produces .tmdl files for tables, relationships, and measures.
"""

import json
from pathlib import Path


def generate_semantic_model(industry_config: dict, semantic_model_config: dict,
                            output_dir: Path,
                            writeback_config: dict | None = None) -> dict[str, list[Path]]:
    """Generate the complete TMDL semantic model.

    Args:
        industry_config: Parsed industry.json content.
        semantic_model_config: Parsed semantic-model.json content.
        output_dir: Demo output root directory.
        writeback_config: Optional writeback-config.json content for SQL Database tables.

    Returns:
        Dict with keys 'tables', 'relationships', 'measures', 'model' → list of paths.
    """
    sm = semantic_model_config.get("semanticModel", {})
    model_name = sm.get("name", "DemoModel")

    industry = industry_config.get("industry", {})
    company_name = industry.get("name", "Demo")

    # Output directory structure matches Power BI .pbip convention
    sm_dir = output_dir / f"{company_name.replace(' ', '')}Model.SemanticModel" / "definition"
    tables_dir = sm_dir / "tables"
    rels_dir = sm_dir / "relationships"

    tables_dir.mkdir(parents=True, exist_ok=True)
    rels_dir.mkdir(parents=True, exist_ok=True)

    result = {"tables": [], "relationships": [], "measures": [], "model": []}

    # 1. Generate model.tmdl (root) — pure Direct Lake, no writeback
    model_path = _generate_model_file(sm_dir, sm, company_name,
                                       has_writeback=False)
    result["model"].append(model_path)

    # 2. Generate table .tmdl files
    tables = sm.get("tables", [])
    measures = sm.get("measures", [])
    measures_by_table = _group_measures_by_table(measures)

    for table in tables:
        path = _generate_table_tmdl(tables_dir, table, measures_by_table.get(table["name"], []))
        result["tables"].append(path)

    # 3. Generate relationship .tmdl files
    relationships = sm.get("relationships", [])
    for i, rel in enumerate(relationships):
        path = _generate_relationship_tmdl(rels_dir, rel, i + 1)
        result["relationships"].append(path)

    # 4. Generate .pbism file
    pbism_path = sm_dir.parent / "definition.pbism"
    _generate_pbism(pbism_path, model_name)
    result["model"].append(pbism_path)

    return result


def generate_writeback_model(industry_config: dict, writeback_config: dict,
                             output_dir: Path) -> dict[str, list[Path]]:
    """Generate a separate DirectQuery semantic model for writeback tables.

    This model connects to the Fabric SQL Database and is kept separate from
    the main Direct Lake model because Fabric does not allow mixing Direct Lake
    and DirectQuery storage modes in the same model.
    """
    wb = writeback_config.get("writebackConfig", writeback_config)
    if not wb.get("enabled", True):
        return {"tables": [], "model": []}

    industry = industry_config.get("industry", {})
    company_name = industry.get("name", "Demo").replace(" ", "")
    model_name = f"{company_name}WritebackModel"

    sm_dir = output_dir / f"{model_name}.SemanticModel" / "definition"
    tables_dir = sm_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, list[Path]] = {"tables": [], "model": []}

    # model.tmdl with WritebackQuery expression only
    model_content = f"""model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tannotation PBI_QueryOrder = {{"tables":[]}}

expression WritebackQuery =
\t\tlet
\t\t\tdatabase = Sql.Database("{{{{SQLDB_SERVER}}}}", "{{{{SQLDB_NAME}}}}")
\t\tin
\t\t\tdatabase
\tlineageTag: {_pseudo_guid(model_name + '_wb_expr')}
"""
    model_path = sm_dir / "model.tmdl"
    with open(model_path, "w", encoding="utf-8") as f:
        f.write(model_content)
    result["model"].append(model_path)

    # Writeback table .tmdl files
    wb_schema = wb.get("schema", "writeback")
    for wb_table in wb.get("tables", []):
        path = _generate_writeback_table_tmdl(tables_dir, wb_table, wb_schema)
        result["tables"].append(path)

    # .pbism file
    pbism_path = sm_dir.parent / "definition.pbism"
    _generate_pbism(pbism_path, model_name)
    result["model"].append(pbism_path)

    return result


def _generate_model_file(sm_dir: Path, sm_config: dict, company_name: str,
                         has_writeback: bool = False) -> Path:
    """Generate the root model.tmdl file."""
    model_name = sm_config.get("name", f"{company_name}Model")
    mode = sm_config.get("mode", "DirectLake")

    content = f"""model Model
\tculture: en-US
\tdefaultPowerBIDataSourceVersion: powerBI_V3
\tannotation PBI_QueryOrder = {{"tables":[]}}

expression DatabaseQuery =
\t\tlet
\t\t\tdatabase = Sql.Database("{{{{WORKSPACE_ID}}}}", "{{{{GOLD_LH_ID}}}}")
\t\tin
\t\t\tdatabase
\tlineageTag: {_pseudo_guid(model_name + '_expr')}
"""

    if has_writeback:
        content += f"""
expression WritebackQuery =
\t\tlet
\t\t\tdatabase = Sql.Database("{{{{SQLDB_SERVER}}}}", "{{{{SQLDB_NAME}}}}")
\t\tin
\t\t\tdatabase
\tlineageTag: {_pseudo_guid(model_name + '_wb_expr')}
"""

    path = sm_dir / "model.tmdl"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _generate_table_tmdl(tables_dir: Path, table: dict,
                          measures: list[dict]) -> Path:
    """Generate a single table .tmdl file."""
    table_name = table["name"]
    schema = table.get("schema", "dbo")
    table_type = table.get("tableType", "")
    hidden = table.get("hidden", False)
    columns = table.get("columns", [])

    lines = [f'table {_tmdl_name(table_name)}']
    lines.append(f'\tlineageTag: {_pseudo_guid(table_name)}')

    if hidden:
        lines.append('\tisHidden')

    lines.append("")

    # Columns
    for col in columns:
        lines.extend(_format_column(col))
        lines.append("")

    # Measures
    for measure in measures:
        lines.extend(_format_measure(measure))
        lines.append("")

    # Partition (Direct Lake)
    lines.append(f'\tpartition {table_name} = entity')
    lines.append('\t\tmode: directLake')
    lines.append('\t\tsource')
    lines.append(f'\t\t\tentityName: {table_name}')
    lines.append(f'\t\t\tschemaName: {schema}')
    lines.append(f'\t\t\texpressionSource: DatabaseQuery')
    lines.append("")

    content = "\n".join(lines)
    path = tables_dir / f"{table_name}.tmdl"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


# ── Spark-to-TMDL type mapping for writeback tables ──

_SPARK_TO_TMDL_TYPES = {
    "STRING": "String",
    "INT": "Int64",
    "BIGINT": "Int64",
    "DECIMAL(18,2)": "Decimal",
    "DECIMAL(5,2)": "Decimal",
    "TIMESTAMP": "DateTime",
    "BOOLEAN": "Boolean",
    "DOUBLE": "Double",
    "FLOAT": "Double",
    "DATE": "DateTime",
}


def _generate_writeback_table_tmdl(tables_dir: Path, wb_table: dict,
                                    wb_schema: str) -> Path:
    """Generate a writeback table .tmdl using DirectQuery to SQL Database."""
    table_name = wb_table["name"]
    columns = wb_table.get("columns", [])

    lines = [f'table {_tmdl_name(table_name)}']
    lines.append(f'\tlineageTag: {_pseudo_guid("wb_" + table_name)}')
    lines.append("")

    # Columns — map from Spark types to TMDL types
    for col in columns:
        tmdl_type = _SPARK_TO_TMDL_TYPES.get(col["dataType"], "String")
        lines.append(f'\tcolumn {_tmdl_name(col["name"])}')
        lines.append(f'\t\tdataType: {tmdl_type}')
        lines.append(f'\t\tlineageTag: {_pseudo_guid("wb_" + table_name + "_" + col["name"])}')
        lines.append(f'\t\tsummarizeBy: None')
        lines.append(f'\t\tsourceColumn: {col["name"]}')
        if col.get("isKey"):
            lines.append('\t\tisKey')
        lines.append("")

    # Partition — DirectQuery to the SQL Database via WritebackQuery expression
    lines.append(f'\tpartition {table_name} = m')
    lines.append('\t\tmode: directQuery')
    lines.append('\t\tsource =')
    lines.append(f'\t\t\tlet')
    lines.append(f'\t\t\t\tSource = WritebackQuery,')
    lines.append(f'\t\t\t\t{wb_schema}_{table_name} = Source{{[Schema="{wb_schema}",Item="{table_name}"]}}[Data]')
    lines.append(f'\t\t\tin')
    lines.append(f'\t\t\t\t{wb_schema}_{table_name}')
    lines.append("")

    content = "\n".join(lines)
    path = tables_dir / f"{table_name}.tmdl"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _format_column(col: dict) -> list[str]:
    """Format a column definition as TMDL lines."""
    name = col["name"]
    data_type = col.get("dataType", "String")
    source_col = col.get("sourceColumn", name)
    is_key = col.get("isKey", False)
    hidden = col.get("hidden", False)
    summarize = col.get("summarizeBy", "")
    format_str = col.get("formatString", "")
    sort_by = col.get("sortByColumn", "")

    lines = [f'\tcolumn {_tmdl_name(name)}']
    lines.append(f'\t\tdataType: {data_type}')
    lines.append(f'\t\tlineageTag: {_pseudo_guid(name)}')
    lines.append(f'\t\tsummarizeBy: {summarize}' if summarize else f'\t\tsummarizeBy: None')
    lines.append(f'\t\tsourceColumn: {source_col}')

    if is_key:
        lines.append('\t\tisKey')
    if hidden:
        lines.append('\t\tisHidden')
    if format_str:
        lines.append(f'\t\tformatString: {format_str}')
    if sort_by:
        lines.append(f'\t\tsortByColumn: {sort_by}')

    return lines


def _format_measure(measure: dict) -> list[str]:
    """Format a measure definition as TMDL lines."""
    name = measure["name"]
    expression = measure.get("expression", "")
    format_str = measure.get("formatString", "")
    folder = measure.get("displayFolder", "")
    description = measure.get("description", "")
    hidden = measure.get("hidden", False)

    lines = [f'\tmeasure {_tmdl_name(name)} =']

    # Multi-line expressions
    expr_lines = expression.strip().split("\n")
    if len(expr_lines) == 1:
        lines[-1] += f" {expr_lines[0]}"
    else:
        for expr_line in expr_lines:
            lines.append(f"\t\t{expr_line}")

    lines.append(f'\t\tlineageTag: {_pseudo_guid(name)}')

    if format_str:
        lines.append(f'\t\tformatString: {format_str}')
    if folder:
        lines.append(f'\t\tdisplayFolder: {folder}')
    if description:
        lines.append(f'\t\tdescription: {description}')
    if hidden:
        lines.append('\t\tisHidden')

    return lines


def _generate_relationship_tmdl(rels_dir: Path, rel: dict, index: int) -> Path:
    """Generate a single relationship .tmdl file."""
    from_table = rel["fromTable"]
    from_col = rel["fromColumn"]
    to_table = rel["toTable"]
    to_col = rel["toColumn"]
    cardinality = rel.get("cardinality", "ManyToOne")
    cross_filter_raw = rel.get("crossFilteringBehavior", "SingleDirection")
    # Map config values to TMDL enum values
    cross_filter_map = {
        "SingleDirection": "oneDirection",
        "BothDirections": "bothDirections",
        "Automatic": "automatic",
    }
    cross_filter = cross_filter_map.get(cross_filter_raw, "oneDirection")
    is_active = rel.get("isActive", True)

    rel_name = f"{from_table}_{from_col}_{to_table}_{to_col}"

    lines = [f"relationship {rel_name}"]

    if not is_active:
        lines.append("\tisActive: false")

    # From side (many)
    lines.append(f"\tfromColumn: {_tmdl_name(from_table)}.{_tmdl_name(from_col)}")
    lines.append(f"\ttoColumn: {_tmdl_name(to_table)}.{_tmdl_name(to_col)}")
    lines.append(f"\tcrossFilteringBehavior: {cross_filter}")
    lines.append("")

    content = "\n".join(lines)

    # Use index-based filename to avoid collisions
    path = rels_dir / f"rel_{index:03d}_{from_table}_to_{to_table}.tmdl"
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)
    return path


def _generate_pbism(path: Path, model_name: str):
    """Generate the .pbism file that links report to semantic model."""
    content = {
        "version": "4.0",
        "settings": {}
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(content, f, indent=2)


def _group_measures_by_table(measures: list[dict]) -> dict[str, list[dict]]:
    """Group measures by their target table."""
    by_table = {}
    for m in measures:
        table = m.get("table", "_Measures")
        by_table.setdefault(table, []).append(m)
    return by_table


def _pseudo_guid(seed_str: str) -> str:
    """Generate a deterministic GUID-like string from a seed (for lineageTag).

    Not a real UUID — just deterministic hex for idempotent generation.
    """
    import hashlib
    h = hashlib.md5(seed_str.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


def _tmdl_name(name: str) -> str:
    """Quote a TMDL identifier if it contains spaces or special characters."""
    if " " in name or "'" in name or any(c in name for c in "[](){},.;:="):
        escaped = name.replace("'", "''")
        return f"'{escaped}'"
    return name
