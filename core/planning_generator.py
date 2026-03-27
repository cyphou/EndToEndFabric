"""Planning generator — produces Planning in Fabric IQ tables and notebooks.

Generates:
  - Planning-config.json per industry
  - Planning_SQLSetup.py (creates planning schema + tables)
  - Planning_Populate.py (populates planning tables with scenario data)
"""

import json
from pathlib import Path


def generate_planning(industry_config: dict, planning_config: dict,
                      output_dir: Path) -> list[Path]:
    """Generate planning artifacts for an industry demo."""
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    gold_lh = artifacts.get("lakehouses", {}).get("gold", "GoldLH")

    created = []

    # Write planning config
    planning_dir = output_dir / "Planning"
    planning_dir.mkdir(parents=True, exist_ok=True)

    config_path = planning_dir / "planning-config.json"
    config_path.write_text(
        json.dumps(planning_config, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    created.append(config_path)

    pc = planning_config.get("planningConfig", planning_config)
    params = pc.get("parameters", {})
    models = pc.get("models", [])
    scenarios = params.get("scenarioTypes", ["Base", "Optimistic", "Conservative"])
    growth = params.get("growthAssumptions", {"base": 0.08, "optimistic": 0.15, "conservative": 0.03})

    # Generate Planning_SQLSetup.py
    sql_nb = _build_sql_setup_notebook(company, gold_lh, models)
    sql_path = output_dir / "notebooks" / "Planning_SQLSetup.py"
    sql_path.parent.mkdir(parents=True, exist_ok=True)
    sql_path.write_text(sql_nb, encoding="utf-8")
    created.append(sql_path)

    # Generate Planning_Populate.py
    pop_nb = _build_populate_notebook(company, gold_lh, models, scenarios, growth)
    pop_path = output_dir / "notebooks" / "Planning_Populate.py"
    pop_path.write_text(pop_nb, encoding="utf-8")
    created.append(pop_path)

    return created


def _build_sql_setup_notebook(company: str, gold_lh: str, models: list) -> str:
    """Build Planning_SQLSetup notebook that creates planning schema and tables."""
    create_stmts = []
    for model in models:
        table = model["outputTable"]
        schema = model.get("outputSchema", "planning")
        grain_cols = model.get("grainColumns", [])
        value_col = model.get("valueColumn", "Value")
        date_col = model.get("dateColumn", "PlanMonth")

        cols = [f"    {date_col} DATE"]
        for gc in grain_cols:
            cols.append(f"    {gc} STRING")
        cols.append(f"    {value_col} DECIMAL(18,2)")
        cols.append("    FiscalYear STRING")
        cols.append("    CreatedDate TIMESTAMP")
        if model.get("writeback"):
            cols.append("    LastModifiedBy STRING")
            cols.append("    LastModifiedDate TIMESTAMP")
        col_defs = ",\n".join(cols)

        create_stmts.append(
            f'spark.sql("""\n'
            f'CREATE TABLE IF NOT EXISTS {gold_lh}.{schema}.{table} (\n'
            f'{col_defs}\n'
            f') USING DELTA\n'
            f'""")\n'
            f'print(f"  ✓ {schema}.{table}")'
        )

    all_creates = "\n\n".join(create_stmts)

    return f'''# Fabric Notebook
# {company} — Planning SQL Setup
# Creates planning schema and tables in {gold_lh}.

# CELL 1 — Create planning schema
spark.sql("CREATE SCHEMA IF NOT EXISTS {gold_lh}.planning")
print("Schema ready: {gold_lh}.planning")

# CELL 2 — Create planning tables
print("\\nCreating planning tables...")

{all_creates}

print(f"\\nPlanning setup complete: {len(models)} tables created")
'''


def _build_populate_notebook(company: str, gold_lh: str, models: list,
                             scenarios: list, growth: dict) -> str:
    """Build Planning_Populate notebook that fills planning tables with scenarios."""
    scenario_list = ", ".join(f'"{s}"' for s in scenarios)

    model_blocks = []
    for model in models:
        name = model["name"]
        table = model["outputTable"]
        schema = model.get("outputSchema", "planning")
        grain_cols = model.get("grainColumns", [])
        value_col = model.get("valueColumn", "Value")
        date_col = model.get("dateColumn", "PlanMonth")
        source = model.get("sourceTable")

        grain_str = ", ".join(f'"{c}"' for c in grain_cols)
        source_ref = f'"{source}"' if source else "None"

        model_blocks.append(f'''
# ── {name} ──
print(f"  Populating {table}...")
plan_rows = []
for scenario in SCENARIOS:
    rate = GROWTH_RATES.get(scenario.lower(), 0.08)
    for month_offset in range(HORIZON):
        d = base_date + datetime.timedelta(days=30 * (month_offset + 1))
        base_value = 100000 * (1 + rate) ** (month_offset / 12)
        plan_rows.append(Row(
            {date_col}=d,
            {value_col}=round(base_value, 2),
            FiscalYear=f"FY{{d.year + (1 if d.month >= 7 else 0)}}",
            CreatedDate=datetime.datetime.now(),
            **{{{", ".join(f'"{gc}": "All"' for gc in grain_cols)}, "Scenario" if "Scenario" in [{grain_str}] else None: scenario}} if "Scenario" in [{grain_str}] else {{{", ".join(f'"{gc}": "All"' for gc in grain_cols)}}}
        ))

if plan_rows:
    df = spark.createDataFrame(plan_rows)
    df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \\
        .saveAsTable(f"{gold_lh}.{schema}.{table}")
    print(f"    ✓ {{len(plan_rows)}} rows")
''')

    all_blocks = "\n".join(model_blocks)

    return f'''# Fabric Notebook
# {company} — Planning Populate
# Populates planning tables with scenario data (Base/Optimistic/Conservative).

# CELL 1 — Configuration
import datetime
from pyspark.sql import Row

GOLD_LH = "{gold_lh}"
HORIZON = 12  # months
SCENARIOS = [{scenario_list}]
GROWTH_RATES = {{
    "base": {growth.get("base", 0.08)},
    "optimistic": {growth.get("optimistic", 0.15)},
    "conservative": {growth.get("conservative", 0.03)},
}}
base_date = datetime.date.today().replace(day=1)

print("Planning data generation")
print(f"  Scenarios: {{SCENARIOS}}")
print(f"  Horizon: {{HORIZON}} months")

# CELL 2 — Populate tables
{all_blocks}

print(f"\\nPlanning population complete")
'''
