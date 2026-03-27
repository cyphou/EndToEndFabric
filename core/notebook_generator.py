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
                       output_dir: Path) -> list[Path]:
    """Generate all PySpark notebooks for an industry demo.

    Args:
        industry_config: Parsed industry.json content.
        sample_data_config: Parsed sample-data.json content.
        output_dir: Demo output root directory.

    Returns:
        List of generated notebook file paths.
    """
    notebooks_dir = output_dir / "notebooks"
    notebooks_dir.mkdir(parents=True, exist_ok=True)

    industry = industry_config.get("industry", {})
    artifacts = industry_config.get("fabricArtifacts", {})
    lakehouses = artifacts.get("lakehouses", {})
    schemas = artifacts.get("schemas", {})

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

    # Build read/write blocks per domain
    domain_blocks = []
    for d in domains:
        schema = d["name"].lower()
        table_reads = []
        for t in d.get("tables", []):
            table_reads.append(f'''    print(f"  Processing {t}...")
    df = spark.table("{bronze}.{t}")
    df = df.dropDuplicates()
    df = df.na.drop(how="all")
    row_count = df.count()
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
        .saveAsTable("{silver}.{schema}.{t}")
    results.append({{"table": "{t}", "schema": "{schema}", "rows": row_count}})
    print(f"    → {silver}.{schema}.{t}: {{row_count}} rows")''')
        domain_blocks.append("\n".join(table_reads))

    all_blocks = "\n\n".join(domain_blocks)

    return f'''# Fabric Notebook
# {company} — NB01: Bronze to Silver
# Reads raw tables from {bronze}, applies quality transforms,
# writes to {silver} with domain schemas.

# CELL 1 — Configuration
BRONZE_LH = "{bronze}"
SILVER_LH = "{silver}"

SCHEMA_MAP = {{
{schema_map}
}}

# CELL 2 — Create Silver Schemas
for schema_name in SCHEMA_MAP:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}.{{schema_name}}")
    print(f"Schema ready: {silver}.{{schema_name}}")

# CELL 3 — Bronze → Silver Transform
results = []

{all_blocks}

# CELL 4 — Summary
print(f"\\nBronze → Silver complete: {{len(results)}} tables processed")
for r in results:
    print(f"  {{r['schema']}}.{{r['table']}}: {{r['rows']}} rows")
'''


def _build_nb02_web_enrichment(ctx: dict) -> str:
    """Generate NB02: Web Enrichment notebook."""
    company = ctx["company_name"]
    silver = ctx["silver_lh"]

    return f'''# Fabric Notebook
# {company} — NB02: Web Enrichment
# Fetches external API data and writes to {silver}.web schema.

# CELL 1 — Configuration
SILVER_LH = "{silver}"

# CELL 2 — Create web schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {silver}.web")
print(f"Schema ready: {silver}.web")

# CELL 3 — Web enrichment (placeholder — extend per industry)
# Example: fetch exchange rates, book metadata, weather data, etc.
import json
from pyspark.sql import Row

# Placeholder: create a simple web enrichment table
web_data = [
    Row(SourceName="ExchangeRates", LastRefresh="2024-01-01", Status="OK"),
    Row(SourceName="BookMetadata", LastRefresh="2024-01-01", Status="OK"),
]
df_web = spark.createDataFrame(web_data)
df_web.write.mode("overwrite").format("delta") \\
    .option("overwriteSchema", "true") \\
    .saveAsTable(f"{silver}.web.WebSources")
print(f"Web enrichment: {{len(web_data)}} sources logged")
'''


def _build_nb03_silver_to_gold(ctx: dict) -> str:
    """Generate NB03: Silver → Gold star schema notebook."""
    company = ctx["company_name"]
    silver = ctx["silver_lh"]
    gold = ctx["gold_lh"]
    gold_schemas = ctx.get("gold_schemas", ["dim", "fact", "analytics", "planning"])
    domains = ctx.get("domains", [])

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
        dim_lines.append(f'''    print(f"  Dim: {t}")
    df = spark.table("{silver}.{schema}.{t}")
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
        .saveAsTable("{gold}.dim.{t}")''')

    fact_lines = []
    for schema, t in fact_tables:
        fact_lines.append(f'''    print(f"  Fact: {t}")
    df = spark.table("{silver}.{schema}.{t}")
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
        .saveAsTable("{gold}.fact.{t}")''')

    dim_block = "\n".join(dim_lines) if dim_lines else '    print("  No dimension tables found")'
    fact_block = "\n".join(fact_lines) if fact_lines else '    print("  No fact tables found")'

    schema_creates = "\n".join(
        f'spark.sql("CREATE SCHEMA IF NOT EXISTS {gold}.{s}")\nprint(f"Schema ready: {gold}.{s}")'
        for s in gold_schemas
    )

    return f'''# Fabric Notebook
# {company} — NB03: Silver to Gold
# Builds star schema in {gold} from {silver} domain tables.
# Dimensions → gold.dim, Facts → gold.fact

# CELL 1 — Configuration
SILVER_LH = "{silver}"
GOLD_LH = "{gold}"

# CELL 2 — Create Gold Schemas
{schema_creates}

# CELL 3 — Generate DimDate (if not exists)
from pyspark.sql.functions import col, lit, date_format, dayofweek, month, year, quarter, when
from pyspark.sql.types import DateType
import datetime

start_date = datetime.date(2023, 1, 1)
end_date = datetime.date(2027, 12, 31)
dates = [Row(Date=start_date + datetime.timedelta(days=i))
         for i in range((end_date - start_date).days + 1)]
from pyspark.sql import Row
df_date = spark.createDataFrame(dates)
df_date = df_date \\
    .withColumn("DateKey", (year("Date") * 10000 + month("Date") * 100 + col("Date").cast("date").substr(9, 2).cast("int")).cast("int")) \\
    .withColumn("Year", year("Date")) \\
    .withColumn("Month", month("Date")) \\
    .withColumn("Quarter", quarter("Date")) \\
    .withColumn("DayOfWeek", dayofweek("Date")) \\
    .withColumn("MonthName", date_format("Date", "MMMM")) \\
    .withColumn("FiscalYear", when(month("Date") >= 7, year("Date") + 1).otherwise(year("Date"))) \\
    .withColumn("FiscalQuarter", when(month("Date") >= 7, ((month("Date") - 7) / 3 + 1).cast("int")).otherwise(((month("Date") + 5) / 3 + 1).cast("int")))

df_date.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
    .saveAsTable(f"{gold}.dim.DimDate")
print(f"DimDate: {{df_date.count()}} rows")

# CELL 4 — Dimensions (Silver → Gold)
print("\\nBuilding dimensions...")
{dim_block}

# CELL 5 — Facts (Silver → Gold)
print("\\nBuilding facts...")
{fact_block}

# CELL 6 — Summary
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
