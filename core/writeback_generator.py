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

    # 4. Generate stored procedure SQL files (Spark SQL / Lakehouse)
    for proc in procedures:
        sql = _build_stored_procedure(gold_lh, schema, proc, tables)
        proc_path = sp_dir / f"{proc['name']}.sql"
        proc_path.write_text(sql, encoding="utf-8")
        created.append(proc_path)

    # 5. Generate Fabric SQL Database DDL (T-SQL for writeback)
    sqldb_dir = wb_dir / "sqldb"
    sqldb_dir.mkdir(parents=True, exist_ok=True)

    # Combined setup script
    setup_sql = _build_sqldb_setup(schema, tables, procedures)
    setup_path = sqldb_dir / "setup_writeback.sql"
    setup_path.write_text(setup_sql, encoding="utf-8")
    created.append(setup_path)

    # 6. Generate SQL Database setup notebook
    sqldb_nb = _build_sqldb_notebook(company, schema, tables, procedures)
    sqldb_nb_path = nb_dir / "09_SQLDatabaseSetup.py"
    sqldb_nb_path.write_text(sqldb_nb, encoding="utf-8")
    created.append(sqldb_nb_path)

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


# ── Spark-to-SQL type mapping ──

_SPARK_TO_SQL_TYPES = {
    "STRING": "NVARCHAR(500)",
    "INT": "INT",
    "BIGINT": "BIGINT",
    "DECIMAL(18,2)": "DECIMAL(18,2)",
    "DECIMAL(5,2)": "DECIMAL(5,2)",
    "TIMESTAMP": "DATETIME2",
    "BOOLEAN": "BIT",
    "DOUBLE": "FLOAT",
    "FLOAT": "REAL",
    "DATE": "DATE",
}


def _sql_type(spark_type: str) -> str:
    """Convert a Spark data type to a T-SQL data type."""
    return _SPARK_TO_SQL_TYPES.get(spark_type, "NVARCHAR(500)")


def _build_sqldb_setup(schema: str, tables: list, procedures: list) -> str:
    """Build the combined T-SQL DDL script for Fabric SQL Database."""
    parts: list[str] = [
        "-- ============================================================",
        "-- Writeback SQL Database Setup",
        "-- Creates schema, tables, and stored procedures for",
        "-- Power BI writeback (translytical) scenarios.",
        "-- ============================================================",
        "",
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}')",
        f"    EXEC('CREATE SCHEMA [{schema}]');",
        "GO",
        "",
    ]

    # CREATE TABLE statements
    for table in tables:
        name = table["name"]
        columns = table.get("columns", [])
        key_cols = [c["name"] for c in columns if c.get("isKey")]

        col_defs = []
        for col in columns:
            sql_t = _sql_type(col["dataType"])
            null_clause = "NOT NULL" if col.get("isKey") else "NULL"
            default = ""
            if "default" in col:
                default = f" DEFAULT '{col['default']}'"
            col_defs.append(f"    [{col['name']}] {sql_t} {null_clause}{default}")

        pk = ""
        if key_cols:
            pk_cols = ", ".join(f"[{k}]" for k in key_cols)
            pk = f",\n    CONSTRAINT PK_{name} PRIMARY KEY ({pk_cols})"

        parts.append(f"-- Table: {schema}.{name}")
        parts.append(
            f"IF NOT EXISTS (SELECT 1 FROM sys.tables t "
            f"JOIN sys.schemas s ON t.schema_id = s.schema_id "
            f"WHERE s.name = '{schema}' AND t.name = '{name}')"
        )
        parts.append("BEGIN")
        parts.append(f"    CREATE TABLE [{schema}].[{name}] (")
        parts.append(",\n".join(col_defs) + pk)
        parts.append("    );")
        parts.append("END")
        parts.append("GO")
        parts.append("")

    # CREATE PROCEDURE statements
    for proc in procedures:
        table_name = proc["table"]
        proc_name = proc["name"]
        key_cols = proc.get("keyColumns", [])
        table_def = next((t for t in tables if t["name"] == table_name), None)
        if not table_def:
            continue

        columns = table_def.get("columns", [])
        # Build parameter list
        params = []
        for col in columns:
            sql_t = _sql_type(col["dataType"])
            default = ""
            if "default" in col:
                default = f" = '{col['default']}'"
            elif not col.get("isKey"):
                default = " = NULL"
            params.append(f"    @{col['name']} {sql_t}{default}")

        param_str = ",\n".join(params)

        # MERGE statement
        key_cond = " AND ".join(
            f"target.[{k}] = source.[{k}]" for k in key_cols
        )
        col_names = [c["name"] for c in columns]
        update_cols = [c for c in col_names if c not in key_cols]
        update_set = ",\n            ".join(
            f"target.[{c}] = source.[{c}]" for c in update_cols
        )
        insert_cols = ", ".join(f"[{c}]" for c in col_names)
        insert_vals = ", ".join(f"source.[{c}]" for c in col_names)
        source_cols = ", ".join(f"@{c} AS [{c}]" for c in col_names)

        parts.append(f"-- Stored Procedure: {proc_name}")
        parts.append(f"CREATE OR ALTER PROCEDURE [{schema}].[{proc_name}]")
        parts.append(param_str)
        parts.append("AS")
        parts.append("BEGIN")
        parts.append("    SET NOCOUNT ON;")
        parts.append("")
        parts.append(f"    MERGE [{schema}].[{table_name}] AS target")
        parts.append(f"    USING (SELECT {source_cols}) AS source")
        parts.append(f"    ON {key_cond}")
        parts.append("    WHEN MATCHED THEN")
        parts.append(f"        UPDATE SET {update_set}")
        parts.append("    WHEN NOT MATCHED THEN")
        parts.append(f"        INSERT ({insert_cols})")
        parts.append(f"        VALUES ({insert_vals});")
        parts.append("END")
        parts.append("GO")
        parts.append("")

    return "\n".join(parts)


def _build_sqldb_notebook(company: str, schema: str,
                          tables: list, procedures: list) -> str:
    """Build NB09: SQL Database Writeback Setup notebook.

    Generates PySpark code that connects to the Fabric SQL Database
    via pyodbc and executes the CREATE TABLE / CREATE PROCEDURE DDL.
    """
    # Build CREATE TABLE DDL strings
    create_table_blocks = []
    for table in tables:
        name = table["name"]
        columns = table.get("columns", [])
        key_cols = [c["name"] for c in columns if c.get("isKey")]

        col_defs = []
        for col in columns:
            sql_t = _sql_type(col["dataType"])
            null_clause = "NOT NULL" if col.get("isKey") else "NULL"
            default = ""
            if "default" in col:
                default = f" DEFAULT '{col['default']}'"
            col_defs.append(f"[{col['name']}] {sql_t} {null_clause}{default}")

        pk = ""
        if key_cols:
            pk_cols = ", ".join(f"[{k}]" for k in key_cols)
            pk = f", CONSTRAINT PK_{name} PRIMARY KEY ({pk_cols})"

        col_defs_str = ", ".join(col_defs)
        create_table_blocks.append(
            f'    {{\n'
            f'        "name": "{name}",\n'
            f'        "ddl": (\n'
            f'            "IF NOT EXISTS (SELECT 1 FROM sys.tables t "\n'
            f'            "JOIN sys.schemas s ON t.schema_id = s.schema_id "\n'
            f'            "WHERE s.name = \'{schema}\' AND t.name = \'{name}\') "\n'
            f'            "CREATE TABLE [{schema}].[{name}] ({col_defs_str}{pk})"\n'
            f'        ),\n'
            f'    }},'
        )

    table_defs = "\n".join(create_table_blocks)

    # Build CREATE PROCEDURE DDL entries
    proc_blocks = []
    for proc in procedures:
        table_name = proc["table"]
        proc_name = proc["name"]
        key_cols = proc.get("keyColumns", [])
        table_def = next((t for t in tables if t["name"] == table_name), None)
        if not table_def:
            continue

        columns = table_def.get("columns", [])
        params = []
        for col in columns:
            sql_t = _sql_type(col["dataType"])
            default = ""
            if "default" in col:
                default = f" = '{col['default']}'"
            elif not col.get("isKey"):
                default = " = NULL"
            params.append(f"@{col['name']} {sql_t}{default}")

        param_str = ", ".join(params)
        col_names = [c["name"] for c in columns]
        update_cols = [c for c in col_names if c not in key_cols]
        key_cond = " AND ".join(f"target.[{k}] = source.[{k}]" for k in key_cols)
        update_set = ", ".join(f"target.[{c}] = source.[{c}]" for c in update_cols)
        insert_cols = ", ".join(f"[{c}]" for c in col_names)
        insert_vals = ", ".join(f"source.[{c}]" for c in col_names)
        source_cols = ", ".join(f"@{c} AS [{c}]" for c in col_names)

        proc_blocks.append(
            f'    {{\n'
            f'        "name": "{proc_name}",\n'
            f'        "ddl": (\n'
            f'            "CREATE OR ALTER PROCEDURE [{schema}].[{proc_name}] "\n'
            f'            "{param_str} "\n'
            f'            "AS BEGIN SET NOCOUNT ON; "\n'
            f'            "MERGE [{schema}].[{table_name}] AS target "\n'
            f'            "USING (SELECT {source_cols}) AS source "\n'
            f'            "ON {key_cond} "\n'
            f'            "WHEN MATCHED THEN UPDATE SET {update_set} "\n'
            f'            "WHEN NOT MATCHED THEN INSERT ({insert_cols}) "\n'
            f'            "VALUES ({insert_vals}); END"\n'
            f'        ),\n'
            f'    }},'
        )

    proc_defs = "\n".join(proc_blocks)

    return f'''# Fabric Notebook
# {company} — NB09: SQL Database Writeback Setup
# Connects to {company}WritebackDB and creates the writeback
# schema, tables, and stored procedures via pyodbc.

# CELL 1 — Configuration
DATABASE_NAME = "{company}WritebackDB"
SCHEMA_NAME = "{schema}"

# Fabric SQL Database endpoint — update with your workspace SQL endpoint
# The notebook runs inside Fabric, so it can use AAD passthrough auth.
print(f"Target database: {{DATABASE_NAME}}")
print(f"Schema: {{SCHEMA_NAME}}")

# CELL 2 — Connect to SQL Database
import struct
import pyodbc

# Fabric notebooks: use the built-in SQL endpoint with AAD token
# from notebookutils import mssparkutils
# token = mssparkutils.credentials.getToken("https://database.windows.net/")
# In local dev, use connection string with AAD Interactive or Service Principal

# For Fabric runtime: use pyodbc with AAD passthrough
connection_string = (
    f"Driver={{{{ODBC Driver 18 for SQL Server}}}};"
    f"Server={{DATABASE_NAME}}.database.fabric.microsoft.com;"
    f"Database={{DATABASE_NAME}};"
    f"Encrypt=yes;TrustServerCertificate=no;"
    f"Authentication=ActiveDirectoryInteractive;"
)

try:
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()
    print("Connected to SQL Database")
except Exception as e:
    print(f"Connection failed: {{e}}")
    print("Falling back to DDL print mode (run manually via SSMS / Azure Data Studio)")
    conn = None
    cursor = None

# CELL 3 — Create Schema
schema_ddl = (
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = '{schema}') "
    "EXEC('CREATE SCHEMA [{schema}]')"
)
if cursor:
    cursor.execute(schema_ddl)
    conn.commit()
    print(f"Schema [{schema}] ready")
else:
    print(f"DDL: {{schema_ddl}}")

# CELL 4 — Create Tables
table_definitions = [
{table_defs}
]

for tbl in table_definitions:
    if cursor:
        try:
            cursor.execute(tbl["ddl"])
            conn.commit()
            print(f"  ✓ Table [{schema}].[{{tbl['name']}}] created/verified")
        except Exception as e:
            print(f"  ✗ Table {{tbl['name']}}: {{e}}")
    else:
        print(f"DDL for {{tbl['name']}}:")
        print(f"  {{tbl['ddl']}}")

# CELL 5 — Create Stored Procedures
procedure_definitions = [
{proc_defs}
]

for proc in procedure_definitions:
    if cursor:
        try:
            cursor.execute(proc["ddl"])
            conn.commit()
            print(f"  ✓ Procedure [{schema}].[{{proc['name']}}] created/updated")
        except Exception as e:
            print(f"  ✗ Procedure {{proc['name']}}: {{e}}")
    else:
        print(f"DDL for {{proc['name']}}:")
        print(f"  {{proc['ddl']}}")

# CELL 6 — Verify
if cursor:
    cursor.execute(
        f"SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
        f"WHERE TABLE_SCHEMA = '{schema}'"
    )
    tables_found = [row[0] for row in cursor.fetchall()]
    cursor.execute(
        f"SELECT ROUTINE_NAME FROM INFORMATION_SCHEMA.ROUTINES "
        f"WHERE ROUTINE_SCHEMA = '{schema}' AND ROUTINE_TYPE = 'PROCEDURE'"
    )
    procs_found = [row[0] for row in cursor.fetchall()]
    conn.close()

    print(f"\\n{{\"=\" * 60}}")
    print(f"  SQL Database Setup Complete")
    print(f"{{\"=\" * 60}}")
    print(f"  Tables: {{len(tables_found)}} ({{', '.join(tables_found)}})")
    print(f"  Procedures: {{len(procs_found)}} ({{', '.join(procs_found)}})")
else:
    print(f"\\nSetup printed in DDL-only mode.")
    print(f"Execute the DDL in SSMS or Azure Data Studio against {{DATABASE_NAME}}.")
    print(f"  Tables: {len(tables)}")
    print(f"  Procedures: {len(procedures)}")
'''
