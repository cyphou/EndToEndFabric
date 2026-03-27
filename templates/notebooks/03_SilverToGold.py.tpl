# Fabric Notebook
# {{company_name}} — NB03: Silver to Gold
# Builds star schema in {{gold_lh}} from {{silver_lh}}.

# CELL 1 — Configuration
SILVER_LH = "{{silver_lh}}"
GOLD_LH = "{{gold_lh}}"

# CELL 2 — Create Gold Schemas
{{#EACH gold_schemas}}
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_LH}.{{item}}")
print(f"Schema ready: {GOLD_LH}.{{item}}")
{{/EACH gold_schemas}}

# CELL 3 — Promote dimensions to Gold
print("Promoting dimension tables...")
{{#EACH dim_tables}}
df = spark.table(f"{SILVER_LH}.{{item.schema}}.{{item.table}}")
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_LH}.dim.{{item.table}}")
print(f"  Dim: {{item.table}}")
{{/EACH dim_tables}}

# CELL 4 — Promote facts to Gold
print("Promoting fact tables...")
{{#EACH fact_tables}}
df = spark.table(f"{SILVER_LH}.{{item.schema}}.{{item.table}}")
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_LH}.fact.{{item.table}}")
print(f"  Fact: {{item.table}}")
{{/EACH fact_tables}}

# CELL 5 — Summary
print(f"\nSilver → Gold complete")
