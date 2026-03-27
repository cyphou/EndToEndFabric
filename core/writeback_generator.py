"""Writeback generator — produces writeback notebooks, stored procedures, and report page.

Generates:
  Writeback/
    writeback-config.json          — Copy of industry writeback config
    NB07_WritebackSetup.py         — Creates writeback schema + Delta tables
    NB08_WritebackAPI.py           — REST API-callable notebook for upserts
    stored_procedures/             — SQL stored procedure definitions
      usp_<Name>.sql
  notebooks/
    07_WritebackSetup.py           — Notebook for Fabric deployment
    08_WritebackAPI.py             — Notebook for Fabric deployment
"""

import json
from pathlib import Path


def generate_writeback(industry_config: dict, writeback_config: dict,
                       output_dir: Path) -> list[Path]:
    """Generate writeback artifacts for an industry demo.

    Args:
        industry_config: Parsed industry.json content.
        writeback_config: Parsed writeback-config.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    gold_lh = artifacts.get("lakehouses", {}).get("gold", "GoldLH")

    wb = writeback_config.get("writebackConfig", writeback_config)
    if not wb.get("enabled", True):
        return []

    schema = wb.get("schema", "writeback")
    tables = wb.get("tables", [])
    procedures = wb.get("storedProcedures", [])

    created: list[Path] = []

    # Writeback output directory
    wb_dir = output_dir / "Writeback"
    wb_dir.mkdir(parents=True, exist_ok=True)

    sp_dir = wb_dir / "stored_procedures"
    sp_dir.mkdir(parents=True, exist_ok=True)

    nb_dir = output_dir / "notebooks"
    nb_dir.mkdir(parents=True, exist_ok=True)

    # 1. Copy writeback config
    config_path = wb_dir / "writeback-config.json"
    config_path.write_text(
        json.dumps(writeback_config, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    created.append(config_path)

    # 2. Generate setup notebook
    setup_nb = _build_setup_notebook(company, gold_lh, schema, tables)
    setup_path = nb_dir / "07_WritebackSetup.py"
    setup_path.write_text(setup_nb, encoding="utf-8")
    created.append(setup_path)

    # 3. Generate API notebook
    api_nb = _build_api_notebook(company, gold_lh, schema, tables, procedures)
    api_path = nb_dir / "08_WritebackAPI.py"
    api_path.write_text(api_nb, encoding="utf-8")
    created.append(api_path)

    # 4. Generate stored procedure SQL files
    for proc in procedures:
        sql = _build_stored_procedure(gold_lh, schema, proc, tables)
        proc_path = sp_dir / f"{proc['name']}.sql"
        proc_path.write_text(sql, encoding="utf-8")
        created.append(proc_path)

    return created


def _build_setup_notebook(company: str, gold_lh: str, schema: str,
                          tables: list) -> str:
    """Build the writeback setup notebook that creates schema + Delta tables."""
    create_blocks = []
    for table in tables:
        name = table["name"]
        columns = table.get("columns", [])
        partition_by = table.get("partitionBy", [])

        col_defs = []
        for col in columns:
            col_defs.append(f"    {col['name']} {col['dataType']}")
        col_str = ",\n".join(col_defs)

        partition_clause = ""
        if partition_by:
            partition_clause = f"\nPARTITIONED BY ({', '.join(partition_by)})"

        create_blocks.append(
            f'# ── {name} ──\n'
            f'spark.sql("""\n'
            f'CREATE TABLE IF NOT EXISTS {gold_lh}.{schema}.{name} (\n'
            f'{col_str}\n'
            f') USING DELTA{partition_clause}\n'
            f'""")\n'
            f'print(f"  ✓ {schema}.{name}")'
        )

    all_creates = "\n\n".join(create_blocks)

    return f'''# Fabric Notebook
# {company} — Writeback Setup
# Creates the writeback schema and Delta tables in {gold_lh}.
# These tables enable Power BI writeback scenarios where users
# can edit data directly from reports.

# COMMAND ----------

# CELL 1 — Create writeback schema
spark.sql("CREATE SCHEMA IF NOT EXISTS {gold_lh}.{schema}")
print("Schema ready: {gold_lh}.{schema}")

# COMMAND ----------

# CELL 2 — Create writeback tables
print("\\nCreating writeback tables...")

{all_creates}

print(f"\\nWriteback setup complete: {len(tables)} tables created")

# COMMAND ----------

# CELL 3 — Verify tables
print("\\nVerifying writeback tables...")
tables_df = spark.sql("SHOW TABLES IN {gold_lh}.{schema}")
tables_df.show(truncate=False)
print("Writeback schema ready for Power BI report integration.")
'''


def _build_api_notebook(company: str, gold_lh: str, schema: str,
                        tables: list, procedures: list) -> str:
    """Build the writeback API notebook with upsert functions."""
    # Build upsert function for each procedure
    func_blocks = []
    for proc in procedures:
        table_name = proc["table"]
        key_cols = proc.get("keyColumns", [])

        # Find matching table definition
        table_def = next((t for t in tables if t["name"] == table_name), None)
        if not table_def:
            continue

        columns = table_def.get("columns", [])
        col_names = [c["name"] for c in columns]
        func_name = proc["name"].replace("usp_", "").lower()

        key_condition = " AND ".join(
            f"target.{k} = source.{k}" for k in key_cols
        )
        update_cols = [c for c in col_names if c not in key_cols]
        update_set = ", ".join(f"target.{c} = source.{c}" for c in update_cols)
        insert_cols = ", ".join(col_names)
        insert_vals = ", ".join(f"source.{c}" for c in col_names)

        func_blocks.append(f'''
def {func_name}(data: list[dict]) -> dict:
    """Upsert rows into {schema}.{table_name}.

    Args:
        data: List of row dicts with keys: {col_names}

    Returns:
        dict with rows_affected count.
    """
    from pyspark.sql import Row
    import datetime

    # Add timestamp if not provided
    for row in data:
        if "ModifiedDate" in row and row["ModifiedDate"] is None:
            row["ModifiedDate"] = datetime.datetime.now()

    df = spark.createDataFrame([Row(**r) for r in data])
    df.createOrReplaceTempView("_wb_source")

    spark.sql("""
        MERGE INTO {gold_lh}.{schema}.{table_name} AS target
        USING _wb_source AS source
        ON {key_condition}
        WHEN MATCHED THEN UPDATE SET {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """)

    spark.catalog.dropTempView("_wb_source")
    return {{"rows_affected": len(data), "table": "{schema}.{table_name}"}}
''')

    all_funcs = "\n".join(func_blocks)

    # Build dispatch table
    dispatch_entries = []
    for proc in procedures:
        func_name = proc["name"].replace("usp_", "").lower()
        dispatch_entries.append(f'    "{proc["name"]}": {func_name},')
    dispatch_str = "\n".join(dispatch_entries)

    return f'''# Fabric Notebook
# {company} — Writeback API
# Provides upsert functions for Power BI writeback scenarios.
# Each function performs a MERGE INTO on the corresponding writeback Delta table.

# COMMAND ----------

# CELL 1 — Configuration
GOLD_LH = "{gold_lh}"
SCHEMA = "{schema}"

print("{company} Writeback API")
print(f"  Target: {{GOLD_LH}}.{{SCHEMA}}")

# COMMAND ----------

# CELL 2 — Upsert functions
{all_funcs}

# COMMAND ----------

# CELL 3 — Dispatch table (for REST API / pipeline calls)
WRITEBACK_PROCEDURES = {{
{dispatch_str}
}}

def execute_writeback(procedure_name: str, data: list[dict]) -> dict:
    """Execute a writeback procedure by name.

    Args:
        procedure_name: Name of the stored procedure (e.g. "usp_UpsertBudgetAdjustment")
        data: List of row dicts to upsert.

    Returns:
        dict with execution result.
    """
    func = WRITEBACK_PROCEDURES.get(procedure_name)
    if not func:
        available = list(WRITEBACK_PROCEDURES.keys())
        raise ValueError(f"Unknown procedure '{{procedure_name}}'. Available: {{available}}")
    return func(data)

print(f"Writeback API ready — {{len(WRITEBACK_PROCEDURES)}} procedures registered:")
for name in WRITEBACK_PROCEDURES:
    print(f"  - {{name}}")
'''


def _build_stored_procedure(gold_lh: str, schema: str, proc: dict,
                            tables: list) -> str:
    """Build a SQL stored procedure definition for writeback upsert."""
    table_name = proc["table"]
    proc_name = proc["name"]
    key_cols = proc.get("keyColumns", [])

    table_def = next((t for t in tables if t["name"] == table_name), None)
    if not table_def:
        return f"-- Table {table_name} not found in config\n"

    columns = table_def.get("columns", [])
    col_names = [c["name"] for c in columns]

    # Build MERGE statement
    key_condition = " AND ".join(
        f"target.{k} = source.{k}" for k in key_cols
    )
    update_cols = [c for c in col_names if c not in key_cols]
    update_set = ",\n        ".join(
        f"target.{c} = source.{c}" for c in update_cols
    )
    insert_cols = ", ".join(col_names)
    insert_vals = ", ".join(f"source.{c}" for c in col_names)

    return f'''-- ============================================================
-- {proc_name}
-- Upsert into {schema}.{table_name}
-- Target: {gold_lh}.{schema}.{table_name}
-- ============================================================

MERGE INTO {gold_lh}.{schema}.{table_name} AS target
USING _writeback_staging AS source
ON {key_condition}
WHEN MATCHED THEN
    UPDATE SET
        {update_set}
WHEN NOT MATCHED THEN
    INSERT ({insert_cols})
    VALUES ({insert_vals});
'''
