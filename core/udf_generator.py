"""User Data Functions generator — produces writeback UDF artifacts.

Generates a Fabric User Data Function item that serves as an API bridge
between Power BI translytical apps and the Fabric SQL Database for
writeback operations (upsert, read, list).

Output structure:
  UserDataFunction/
    definition.json         — UDF item definition (runtime, connections, functions, libs)
    resources/functions.json — Function metadata (bindings, parameters, return types)
    function_app.py         — Python source with @udf.function() decorated functions
"""

import json
from pathlib import Path


# SQL type mapping from Spark types for parameterized queries
_SPARK_TO_PY_TYPE = {
    "STRING": "str",
    "INT": "int",
    "BIGINT": "int",
    "DECIMAL(18,2)": "float",
    "DECIMAL(5,2)": "float",
    "TIMESTAMP": "str",
    "BOOLEAN": "bool",
    "DOUBLE": "float",
    "FLOAT": "float",
    "DATE": "str",
}


def generate_udf(industry_config: dict, writeback_config: dict,
                  output_dir: Path) -> list[Path]:
    """Generate User Data Function artifacts for writeback.

    Args:
        industry_config: Parsed industry.json content.
        writeback_config: Parsed writeback-config.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated file paths.
    """
    wb = writeback_config.get("writebackConfig", writeback_config)
    if not wb.get("enabled", True):
        return []

    industry = industry_config.get("industry", {})
    company = industry.get("name", "Demo").replace(" ", "")

    schema = wb.get("schema", "writeback")
    tables = wb.get("tables", [])
    procedures = wb.get("storedProcedures", [])

    udf_dir = output_dir / "UserDataFunction"
    res_dir = udf_dir / "resources"
    res_dir.mkdir(parents=True, exist_ok=True)

    created: list[Path] = []

    # 1. definition.json
    definition = _build_definition(company, tables, procedures)
    def_path = udf_dir / "definition.json"
    def_path.write_text(json.dumps(definition, indent=2), encoding="utf-8")
    created.append(def_path)

    # 2. resources/functions.json
    functions_meta = _build_functions_metadata(tables, procedures, schema)
    meta_path = res_dir / "functions.json"
    meta_path.write_text(json.dumps(functions_meta, indent=2), encoding="utf-8")
    created.append(meta_path)

    # 3. function_app.py
    py_code = _build_function_app(company, schema, tables, procedures)
    py_path = udf_dir / "function_app.py"
    py_path.write_text(py_code, encoding="utf-8")
    created.append(py_path)

    return created


def _build_definition(company: str, tables: list, procedures: list) -> dict:
    """Build the definition.json for the UDF item."""
    # Function list: one upsert + one read per table, plus a list_tables function
    functions = [
        {
            "name": "list_tables",
            "description": "Returns list of available writeback tables",
            "isPublicEndpointEnabled": True,
        }
    ]
    for table in tables:
        name = table["name"]
        functions.append({
            "name": f"upsert_{_snake(name)}",
            "description": f"Upsert a row into {name}",
            "isPublicEndpointEnabled": True,
        })
        functions.append({
            "name": f"read_{_snake(name)}",
            "description": f"Read rows from {name}",
            "isPublicEndpointEnabled": True,
        })

    return {
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/userDataFunction/definition/1.1.0/schema.json",
        "runtime": "PYTHON",
        "connectedDataSources": [
            {
                "alias": "WritebackDB",
                "artifactId": "{{SQLDB_ID}}",
                "artifactType": "SqlDbNative",
                "workspaceId": "{{WORKSPACE_ID}}",
            }
        ],
        "functions": functions,
        "libraries": {
            "public": [
                {"name": "fabric-user-data-functions", "type": "PYPI", "version": "1.0"},
            ],
            "private": [],
        },
    }


def _build_functions_metadata(tables: list, procedures: list, schema: str) -> dict:
    """Build the resources/functions.json metadata."""
    db_alias = None  # Will be set at deploy time
    metadata: list[dict] = []

    # list_tables function — no DB connection needed
    metadata.append({
        "name": "list_tables",
        "scriptFile": "function_app.py",
        "bindings": [
            {
                "methods": ["POST"],
                "route": "",
                "authLevel": "Anonymous",
                "name": "req",
                "direction": "In",
                "type": "HttpTrigger",
            }
        ],
        "fabricProperties": {
            "fabricMetadataSchemaVersion": "1.1.0",
            "fabricFunctionParameters": [],
            "fabricFunctionReturnType": "list",
        },
    })

    # Per-table upsert + read functions
    for table in tables:
        name = table["name"]
        columns = table.get("columns", [])

        # Upsert function parameters (camelCase as required by UDF SDK)
        upsert_params = []
        for col in columns:
            py_type = _SPARK_TO_PY_TYPE.get(col["dataType"], "str")
            upsert_params.append({
                "dataType": py_type,
                "name": _camel(col["name"]),
            })

        # Upsert function
        metadata.append({
            "name": f"upsert_{_snake(name)}",
            "scriptFile": "function_app.py",
            "bindings": [
                {
                    "methods": ["POST"],
                    "route": "",
                    "authLevel": "Anonymous",
                    "name": "req",
                    "direction": "In",
                    "type": "HttpTrigger",
                },
                {
                    "itemType": "SqlDbNative",
                    "subType": "FabricSqlConnection",
                    "alias": "{{CONNECTION_ALIAS}}",
                    "name": "sqlDb",
                    "direction": "In",
                    "type": "FabricItem",
                },
            ],
            "fabricProperties": {
                "fabricMetadataSchemaVersion": "1.1.0",
                "fabricFunctionParameters": upsert_params,
                "fabricFunctionReturnType": "dict",
            },
        })

        # Read function
        metadata.append({
            "name": f"read_{_snake(name)}",
            "scriptFile": "function_app.py",
            "bindings": [
                {
                    "methods": ["POST"],
                    "route": "",
                    "authLevel": "Anonymous",
                    "name": "req",
                    "direction": "In",
                    "type": "HttpTrigger",
                },
                {
                    "itemType": "SqlDbNative",
                    "subType": "FabricSqlConnection",
                    "alias": "{{CONNECTION_ALIAS}}",
                    "name": "sqlDb",
                    "direction": "In",
                    "type": "FabricItem",
                },
            ],
            "fabricProperties": {
                "fabricMetadataSchemaVersion": "1.1.0",
                "fabricFunctionParameters": [
                    {"dataType": "int", "name": "topN"},
                ],
                "fabricFunctionReturnType": "list",
            },
        })

    return {
        "runtime": "PYTHON",
        "functionsMetadata": metadata,
    }


def _build_function_app(company: str, schema: str, tables: list,
                         procedures: list) -> str:
    """Build the function_app.py Python source code."""
    connection_alias = "WritebackDB"

    lines = [
        '"""',
        f"User Data Functions for {company} writeback operations.",
        "",
        "These functions serve as the API bridge between Power BI translytical",
        "apps and the Fabric SQL Database for read/write operations.",
        '"""',
        "",
        "import datetime",
        "import fabric.functions as fn",
        "import logging",
        "",
        "udf = fn.UserDataFunctions()",
        "",
        "",
        "# ── List available writeback tables ──",
        "",
        "@udf.function()",
        "def list_tables() -> list:",
        '    """Return the list of available writeback tables."""',
        "    return [",
    ]

    for table in tables:
        desc = table.get("description", table["name"])
        lines.append(f'        {{"name": "{table["name"]}", "description": "{desc}"}},')

    lines.extend([
        "    ]",
        "",
    ])

    # Per-table upsert + read functions
    proc_by_table = {p["table"]: p for p in procedures}

    for table in tables:
        tname = table["name"]
        columns = table.get("columns", [])
        key_cols = [c for c in columns if c.get("isKey")]
        proc = proc_by_table.get(tname)
        proc_name = proc["name"] if proc else f"usp_Upsert{tname}"

        # Build parameter list for the upsert function
        params = [f"sqlDb: fn.FabricSqlConnection"]
        for col in columns:
            py_type = _SPARK_TO_PY_TYPE.get(col["dataType"], "str")
            default = ""
            if not col.get("isKey"):
                if py_type == "str":
                    default = ' = ""'
                elif py_type == "int":
                    default = " = 0"
                elif py_type == "float":
                    default = " = 0.0"
                elif py_type == "bool":
                    default = " = False"
            params.append(f"{_camel(col['name'])}: {py_type}{default}")
        param_str = ", ".join(params)

        # Build the stored procedure call with parameterized query
        sp_params = ", ".join([f"@{col['name']} = ?" for col in columns])
        sp_args = ", ".join([_camel(col["name"]) for col in columns])

        lines.extend([
            "",
            f"# ── Upsert into {tname} ──",
            "",
            f'@udf.connection(argName="sqlDb", alias="{connection_alias}")',
            "@udf.function()",
            f"def upsert_{_snake(tname)}({param_str}) -> dict:",
            f'    """Upsert a row into {schema}.{tname} via stored procedure."""',
            f"    logging.info('Upserting into {tname}')",
            "    connection = sqlDb.connect()",
            "    cursor = connection.cursor()",
            "    try:",
            f'        cursor.execute(',
            f'            "EXEC {schema}.{proc_name} {sp_params}",',
            f'            ({sp_args},)',
            '        )',
            "        connection.commit()",
            '        return {"status": "ok", "table": "' + tname + '", "operation": "upsert"}',
            "    except Exception as e:",
            f'        logging.error(f"Upsert failed for {tname}: {{e}}")',
            '        raise fn.UserThrownError(f"Upsert failed: {e}", {"table": "' + tname + '"})',
            "    finally:",
            "        cursor.close()",
            "        connection.close()",
            "",
        ])

        # Read function
        lines.extend([
            "",
            f"# ── Read from {tname} ──",
            "",
            f'@udf.connection(argName="sqlDb", alias="{connection_alias}")',
            "@udf.function()",
            f"def read_{_snake(tname)}(sqlDb: fn.FabricSqlConnection, topN: int = 100) -> list:",
            f'    """Read rows from {schema}.{tname}."""',
            f"    logging.info(f'Reading from {tname}, topN={{topN}}')",
            "    connection = sqlDb.connect()",
            "    cursor = connection.cursor()",
            "    try:",
            f'        cursor.execute(f"SELECT TOP ({{topN}}) * FROM {schema}.{tname}")',
            "        columns = [col[0] for col in cursor.description]",
            "        rows = []",
            "        for row in cursor.fetchall():",
            "            item = {}",
            "            for col_name, val in zip(columns, row):",
            "                if isinstance(val, (datetime.date, datetime.datetime)):",
            "                    val = val.isoformat()",
            "                item[col_name] = val",
            "            rows.append(item)",
            "        return rows",
            "    finally:",
            "        cursor.close()",
            "        connection.close()",
            "",
        ])

    return "\n".join(lines) + "\n"


def _snake(name: str) -> str:
    """Convert PascalCase to snake_case."""
    result = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


def _camel(name: str) -> str:
    """Convert PascalCase to camelCase (required by UDF SDK)."""
    if not name:
        return name
    return name[0].lower() + name[1:]
