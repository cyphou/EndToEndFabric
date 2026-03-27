# Fabric Notebook
# {{company_name}} — NB01: Bronze to Silver
# Reads raw tables from {{bronze_lh}}, applies quality transforms,
# writes to {{silver_lh}} with domain schemas.

# CELL 1 — Configuration
BRONZE_LH = "{{bronze_lh}}"
SILVER_LH = "{{silver_lh}}"

# CELL 2 — Create Silver Schemas
{{#EACH silver_schemas}}
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {{silver_lh}}.{{item}}")
print(f"Schema ready: {{silver_lh}}.{{item}}")
{{/EACH silver_schemas}}

# CELL 3 — Bronze → Silver Transform
results = []

{{#EACH domains}}
# Domain: {{item.name}}
{{#EACH item.tables}}
print(f"  Processing {{item}}...")
df = spark.table(f"{BRONZE_LH}.{{item}}")
df = df.dropDuplicates()
df = df.na.drop(how="all")
row_count = df.count()
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_LH}.{{item.schema}}.{{item}}")
results.append({"table": "{{item}}", "schema": "{{item.schema}}", "rows": row_count})
print(f"    → {SILVER_LH}.{{item.schema}}.{{item}}: {row_count} rows")

{{/EACH item.tables}}
{{/EACH domains}}

# CELL 4 — Summary
print(f"\nBronze → Silver complete: {len(results)} tables processed")
for r in results:
    print(f"  {r['schema']}.{r['table']}: {r['rows']} rows")
