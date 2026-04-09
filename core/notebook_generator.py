"""PySpark notebook generator.

Generates Fabric notebooks (NB01–NB06) from industry configs.
Each notebook is a real PySpark script targeting Fabric Lakehouse,
following the Medallion architecture (Bronze → Silver → Gold).
"""

from pathlib import Path

from core.config_loader import PROJECT_ROOT
from core.template_engine import render_template_file


TEMPLATES_DIR = PROJECT_ROOT / "templates" / "notebooks"


def generate_notebooks(industry_config: dict, sample_data_config: dict,
                       output_dir: Path,
                       web_enrichment_config: dict | None = None) -> list[Path]:
    """Generate all PySpark notebooks for an industry demo.

    Args:
        industry_config: Parsed industry.json content.
        sample_data_config: Parsed sample-data.json content.
        output_dir: Demo output root directory.
        web_enrichment_config: Parsed web-enrichment.json content (optional).

    Returns:
        List of generated notebook file paths.
    """
    notebooks_dir = output_dir / "notebooks"
    notebooks_dir.mkdir(parents=True, exist_ok=True)

    industry = industry_config.get("industry", {})
    artifacts = industry_config.get("fabricArtifacts", {})
    lakehouses = artifacts.get("lakehouses", {})
    schemas = artifacts.get("schemas", {})

    we = {}
    if web_enrichment_config:
        we = web_enrichment_config.get("webEnrichment", web_enrichment_config)

    context = {
        "industry": industry,
        "fabricArtifacts": artifacts,
        "lakehouses": lakehouses,
        "schemas": schemas,
        "company_name": industry.get("name", "Demo"),
        "company_id": industry.get("id", "demo"),
        "bronze_lh": lakehouses.get("bronze", "BronzeLH"),
        "silver_lh": lakehouses.get("silver", "SilverLH"),
        "gold_lh": lakehouses.get("gold", "GoldLH"),
        "silver_schemas": schemas.get("silver", []),
        "gold_schemas": schemas.get("gold", []),
        "domains": _extract_domain_info(sample_data_config),
        "web_sources": we.get("sources", []),
        "date_range": industry.get("dateRange", {"start": "2023-01-01", "end": "2027-12-31"}),
        "fiscal_year_start_month": industry.get("fiscalYearStartMonth", 7),
    }

    generated = []

    notebook_specs = [
        ("01_BronzeToSilver", _build_nb01_bronze_to_silver),
        ("02_WebEnrichment", _build_nb02_web_enrichment),
        ("03_SilverToGold", _build_nb03_silver_to_gold),
        ("06_DiagnosticCheck", _build_nb06_diagnostic),
    ]

    for nb_name, builder_fn in notebook_specs:
        tpl_path = TEMPLATES_DIR / f"{nb_name}.py.tpl"
        out_path = notebooks_dir / f"{nb_name}.py"

        if tpl_path.is_file():
            rendered = render_template_file(tpl_path, context)
            out_path.write_text(rendered, encoding="utf-8")
        else:
            code = builder_fn(context)
            out_path.write_text(code, encoding="utf-8")

        generated.append(out_path)

    return generated


def _build_nb01_bronze_to_silver(ctx: dict) -> str:
    """Generate NB01: Bronze → Silver PySpark notebook."""
    company = ctx["company_name"]
    bronze = ctx["bronze_lh"]
    silver = ctx["silver_lh"]
    domains = ctx.get("domains", [])

    # Build schema mapping: domain → [tables]
    schema_lines = []
    for d in domains:
        tables_str = ", ".join(f'"{t}"' for t in d.get("tables", []))
        schema_lines.append(f'    "{d["name"].lower()}": [{tables_str}]')
    schema_map = ",\n".join(schema_lines)

    # Build CSV source map: folder → [tables]
    csv_source_lines = []
    for d in domains:
        folder = d.get("folder") or d.get("name", "")
        tables_str = ", ".join(f'"{t}"' for t in d.get("tables", []))
        csv_source_lines.append(f'    "{folder}": [{tables_str}]')
    csv_source_map = ",\n".join(csv_source_lines)

    # Build Bronze→Silver read/write blocks per domain (abfss paths avoid catalog issues)
    domain_blocks = []
    for d in domains:
        schema = d["name"].lower()
        table_reads = []
        for t in d.get("tables", []):
            block = (
                f'print(f"  Processing {t}...")\n'
                + f'df = spark.read.format("delta").load(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{BRONZE_LH_ID}}}}/Tables/{t}")\n'
                + 'df = df.dropDuplicates()\n'
                + 'df = df.na.drop(how="all")\n'
                + 'row_count = df.count()\n'
                + 'df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\\n'
                + f'    .save(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{SILVER_LH_ID}}}}/Tables/{schema}/{t}")\n'
                + f'results.append({{"table": "{t}", "schema": "{schema}", "rows": row_count}})\n'
                + f'print(f"    -> {schema}/{t}: {{row_count}} rows")'
            )
            table_reads.append(block)
        domain_blocks.append("\n".join(table_reads))

    all_blocks = "\n\n".join(domain_blocks)

    return f'''# Fabric Notebook
# {company} -- NB01: Bronze to Silver
# Step 1: Ingests CSV files from BronzeLH/Files/ into BronzeLH Delta tables.
# Step 2: Reads Bronze tables, applies quality transforms, writes to {silver} with domain schemas.

# CELL 1 -- Configuration
BRONZE_LH = "{bronze}"
SILVER_LH = "{silver}"
WORKSPACE_ID = "{{{{WORKSPACE_ID}}}}"
BRONZE_LH_ID = "{{{{BRONZE_LH_ID}}}}"
SILVER_LH_ID = "{{{{SILVER_LH_ID}}}}"

SCHEMA_MAP = {{
{schema_map}
}}

# CSV_SOURCES maps BronzeLH/Files/ folder names to table lists
CSV_SOURCES = {{
{csv_source_map}
}}

# CELL 2 -- Ingest CSV files into BronzeLH Delta tables
print("Ingesting CSV files from BronzeLH/Files/ into Bronze Delta tables...")
ingest_results = []
for folder, tables in CSV_SOURCES.items():
    for table in tables:
        csv_path = f"abfss://{{WORKSPACE_ID}}@onelake.dfs.fabric.microsoft.com/{{BRONZE_LH_ID}}/Files/{{folder}}/{{table}}.csv"
        print(f"  Reading {{folder}}/{{table}}.csv...")
        try:
            df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)
            row_count = df_csv.count()
            df_csv.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
                .save(f"abfss://{{WORKSPACE_ID}}@onelake.dfs.fabric.microsoft.com/{{BRONZE_LH_ID}}/Tables/{{table}}")
            ingest_results.append({{"folder": folder, "table": table, "rows": row_count}})
            print(f"    -> {{BRONZE_LH}}/{{table}}: {{row_count}} rows")
        except Exception as e:
            print(f"    WARNING: Could not ingest {{table}}: {{e}}")
print(f"\\nIngested {{len(ingest_results)}} tables into BronzeLH.")

# CELL 3 -- Bronze to Silver Transform (schemas created automatically by saveAsTable 3-level naming)
results = []

{all_blocks}

# CELL 4 -- Summary
print(f"\\nBronze \u2192 Silver complete: {{len(results)}} tables processed")
for r in results:
    print(f"  {{r[\x27schema\x27]}}.{{r[\x27table\x27]}}: {{r[\x27rows\x27]}} rows")
'''


def _build_nb02_web_enrichment(ctx: dict) -> str:
    """Generate NB02: Web Enrichment notebook from web-enrichment.json sources."""
    company = ctx["company_name"]
    silver = ctx["silver_lh"]
    sources = ctx.get("web_sources", [])

    # Build per-source data generation blocks
    source_blocks = []
    registry_entries = []
    for src in sources:
        name = src.get("name", "Unknown")
        table_name = src.get("tableName", f"Web{name}")
        description = src.get("description", "")
        endpoint = src.get("endpoint", "")
        columns = src.get("columns", [])
        schedule = src.get("schedule", "daily")
        var = table_name.lower()

        # Build Row fields with realistic sample data per type
        row_fields = []
        for col in columns:
            cname = col["name"]
            ctype = col.get("type", "string")
            if ctype == "date":
                row_fields.append(
                    f'        "{cname}": (datetime.date.today() - datetime.timedelta(days=i)).isoformat()')
            elif ctype in ("double", "float", "number"):
                row_fields.append(
                    f'        "{cname}": round(random.uniform(10.0, 500.0), 2)')
            elif ctype in ("int", "integer"):
                row_fields.append(
                    f'        "{cname}": random.randint(1, 10000)')
            else:
                row_fields.append(
                    f'        "{cname}": f"{cname}_{{i:03d}}"')

        fields_str = ",\n".join(row_fields)

        source_blocks.append(
            f'# -- {name}: {description}\n'
            f'# Production endpoint: {endpoint}\n'
            f'print(f"  Fetching {name}...")\n'
            f'{var}_rows = []\n'
            f'for i in range(30):\n'
            f'    {var}_rows.append({{\n'
            f'{fields_str}\n'
            f'    }})\n'
            f'df_{var} = spark.createDataFrame([Row(**r) for r in {var}_rows])\n'
            f'df_{var}.write.mode("overwrite").format("delta") \\\n'
            f'    .option("overwriteSchema", "true") \\\n'
            f'    .save(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{SILVER_LH_ID}}}}/Tables/web/{table_name}")\n'
            f'print(f"  ok {table_name}: {{len({var}_rows)}} rows")\n'
        )

        registry_entries.append(
            f'    Row(SourceName="{name}", TableName="{table_name}", '
            f'Endpoint="{endpoint}", Schedule="{schedule}", '
            f'LastRefresh=datetime.datetime.now().isoformat(), Status="OK"),'
        )

    if not source_blocks:
        source_blocks.append('    print("  No web enrichment sources configured")')

    all_blocks = "\n".join(source_blocks)
    registry_lines = "\n".join(registry_entries) if registry_entries else (
        '    Row(SourceName="None", TableName="", Endpoint="", '
        'Schedule="", LastRefresh="", Status="N/A"),'
    )

    return f'''# Fabric Notebook
# {company} -- NB02: Web Enrichment
# Fetches and simulates external API data, writes to SilverLH/Tables/web/ via abfss.

# CELL 1 -- Setup
import random
import datetime
from pyspark.sql import Row

SILVER_LH = "{silver}"
WORKSPACE_ID = "{{{{WORKSPACE_ID}}}}"
SILVER_LH_ID = "{{{{SILVER_LH_ID}}}}"

print("=" * 60)
print(f"  {company} Web Enrichment")
print("=" * 60)

# CELL 2 -- Generate web enrichment tables
{all_blocks}

# CELL 3 -- Web sources registry
web_registry = [
{registry_lines}
]
df_registry = spark.createDataFrame(web_registry)
df_registry.write.mode("overwrite").format("delta") \\
    .option("overwriteSchema", "true") \\
    .save(f"abfss://{{WORKSPACE_ID}}@onelake.dfs.fabric.microsoft.com/{{SILVER_LH_ID}}/Tables/web/WebSources")
print(f"\\nWeb sources registry: {{len(web_registry)}} sources logged")

# CELL 4 -- Summary
print(f"\\n{company} web enrichment complete -- {len(sources)} sources")
'''


def _build_nb03_silver_to_gold(ctx: dict) -> str:
    """Generate NB03: Silver → Gold star schema notebook."""
    company = ctx["company_name"]
    silver = ctx["silver_lh"]
    gold = ctx["gold_lh"]
    gold_schemas = ctx.get("gold_schemas", ["dim", "fact", "analytics", "planning"])
    domains = ctx.get("domains", [])
    date_range = ctx.get("date_range", {"start": "2023-01-01", "end": "2027-12-31"})
    fiscal_month = ctx.get("fiscal_year_start_month", 7)

    # Parse date range into year/month/day components
    start_parts = date_range["start"].split("-")
    end_parts = date_range["end"].split("-")
    start_y, start_m, start_d = int(start_parts[0]), int(start_parts[1]), int(start_parts[2])
    end_y, end_m, end_d = int(end_parts[0]), int(end_parts[1]), int(end_parts[2])

    # Pre-compute fiscal offset for FiscalQuarter formula
    fiscal_offset = 12 - fiscal_month + 1  # e.g. month=7 → offset=6

    # Build dimension / fact classification
    dim_tables = []
    fact_tables = []
    for d in domains:
        schema = d["name"].lower()
        for t in d.get("tables", []):
            if t.startswith("Dim"):
                dim_tables.append((schema, t))
            elif t.startswith("Fact"):
                fact_tables.append((schema, t))

    dim_lines = []
    for schema, t in dim_tables:
        dim_lines.append(
            f'print(f"  Dim: {t}")'  "\n"
            f'df = spark.read.format("delta").load(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{SILVER_LH_ID}}}}/Tables/{schema}/{t}")' "\n"
            'df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\' "\n"
            f'    .save(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{GOLD_LH_ID}}}}/Tables/{t}")'
        )

    fact_lines = []
    for schema, t in fact_tables:
        fact_lines.append(
            f'print(f"  Fact: {t}")'  "\n"
            f'df = spark.read.format("delta").load(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{SILVER_LH_ID}}}}/Tables/{schema}/{t}")' "\n"
            'df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\' "\n"
            f'    .save(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{GOLD_LH_ID}}}}/Tables/{t}")'
        )

    dim_block = "\n".join(dim_lines) if dim_lines else 'print("  No dimension tables found")'
    fact_block = "\n".join(fact_lines) if fact_lines else 'print("  No fact tables found")'

    schema_creates = "\n".join(
        f'spark.sql("CREATE SCHEMA IF NOT EXISTS {gold}.{s}")\nprint(f"Schema ready: {gold}.{s}")'
        for s in gold_schemas
    )

    return f'''# Fabric Notebook
# {company} — NB03: Silver to Gold
# Builds star schema in {gold} from {silver} domain tables.
# All Gold tables written to Tables/<TableName> (dbo schema in SQL endpoint).

# CELL 1 -- Configuration
SILVER_LH = "{silver}"
GOLD_LH = "{gold}"
WORKSPACE_ID = "{{{{WORKSPACE_ID}}}}"
SILVER_LH_ID = "{{{{SILVER_LH_ID}}}}"
GOLD_LH_ID = "{{{{GOLD_LH_ID}}}}"

# Note: all Delta writes use flat abfss:// paths (Tables/<TableName>) so the
# Lakehouse SQL Analytics Endpoint auto-discovers them as dbo-schema tables.

# CELL 2 -- Generate DimDate
from pyspark.sql.functions import col, lit, date_format, dayofweek, month, year, quarter, when, isnull
from pyspark.sql.types import DateType
from pyspark.sql import Row
import datetime

start_date = datetime.date({start_y}, {start_m}, {start_d})
end_date = datetime.date({end_y}, {end_m}, {end_d})
dates = [Row(Date=start_date + datetime.timedelta(days=i))
         for i in range((end_date - start_date).days + 1)]
df_date = spark.createDataFrame(dates)
df_date = df_date \\
    .withColumn("DateKey", (year("Date") * 10000 + month("Date") * 100 + col("Date").cast("date").substr(9, 2).cast("int")).cast("int")) \\
    .withColumn("Year", year("Date")) \\
    .withColumn("Month", month("Date")) \\
    .withColumn("Quarter", quarter("Date")) \\
    .withColumn("DayOfWeek", date_format("Date", "EEEE")) \\
    .withColumn("MonthName", date_format("Date", "MMMM")) \\
    .withColumn("FiscalYear", when(month("Date") >= {fiscal_month}, year("Date") + 1).otherwise(year("Date"))) \\
    .withColumn("FiscalQuarter", when(month("Date") >= {fiscal_month}, ((month("Date") - {fiscal_month}) / 3 + 1).cast("int")).otherwise(((month("Date") + {fiscal_offset}) / 3 + 1).cast("int"))) \\
    .withColumn("IsWeekend", (dayofweek("Date").isin([1, 7])).cast("boolean")) \\
    .withColumn("IsHoliday", lit(False).cast("boolean"))

df_date.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
    .save(f"abfss://{{{{WORKSPACE_ID}}}}@onelake.dfs.fabric.microsoft.com/{{{{GOLD_LH_ID}}}}/Tables/DimDate")
print(f"DimDate: {{df_date.count()}} rows")

# CELL 3 -- Dimensions (Silver -> Gold)
print("\\nBuilding dimensions...")
{dim_block}

# CELL 4 -- Facts (Silver -> Gold)
print("\\nBuilding facts...")
{fact_block}

# CELL 5 -- Summary
dim_count = {len(dim_tables)}
fact_count = {len(fact_tables)}
print(f"\\nSilver → Gold complete: {{dim_count}} dims, {{fact_count}} facts + DimDate")
'''


def _build_nb06_diagnostic(ctx: dict) -> str:
    """Generate NB06: Diagnostic Check notebook."""
    company = ctx["company_name"]
    bronze = ctx["bronze_lh"]
    silver = ctx["silver_lh"]
    gold = ctx["gold_lh"]

    return f'''# Fabric Notebook
# {company} — NB06: Diagnostic Check
# Validates data quality and completeness across all Lakehouses.

# CELL 1 — Configuration
LAKEHOUSES = {{
    "Bronze": "{bronze}",
    "Silver": "{silver}",
    "Gold":   "{gold}",
}}

# CELL 2 — Table Inventory
print("=" * 60)
print(f"  {company} — Diagnostic Report")
print("=" * 60)

for layer, lh in LAKEHOUSES.items():
    print(f"\\n{{layer}} Lakehouse ({{lh}}):")
    try:
        tables = spark.sql(f"SHOW TABLES IN {{lh}}").collect()
        for t in tables:
            schema = t["namespace"] if "namespace" in t.asDict() else ""
            name = t["tableName"]
            try:
                count = spark.table(f"{{lh}}.{{schema}}.{{name}}" if schema else f"{{lh}}.{{name}}").count()
                print(f"  {{schema}}.{{name}}: {{count:,}} rows")
            except Exception as e:
                print(f"  {{schema}}.{{name}}: ERROR - {{e}}")
    except Exception as e:
        print(f"  Could not list tables: {{e}}")

# CELL 3 — Null check on key columns
print("\\n" + "=" * 60)
print("  Null Check on Primary Keys")
print("=" * 60)
# Extended by industry-specific checks

print("\\nDiagnostic complete.")
'''


def _extract_domain_info(sample_data_config: dict | None) -> list[dict]:
    """Extract domain names and table names from sample-data.json."""
    if not sample_data_config:
        return []
    domains = sample_data_config.get("sampleData", {}).get("domains", [])
    return [
        {
            "name": d.get("name", ""),
            "folder": d.get("folder", ""),
            "tables": [t["name"] for t in d.get("tables", [])],
        }
        for d in domains
    ]
