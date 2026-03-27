# Fabric Notebook
# {{company_name}} — NB06: Diagnostic Check
# Validates the demo environment health.

# CELL 1 — Configuration
BRONZE_LH = "{{bronze_lh}}"
SILVER_LH = "{{silver_lh}}"
GOLD_LH = "{{gold_lh}}"

# CELL 2 — Check Bronze tables
print("=== Bronze Layer ===")
bronze_tables = spark.catalog.listTables(BRONZE_LH)
for t in bronze_tables:
    count = spark.table(f"{BRONZE_LH}.{t.name}").count()
    status = "OK" if count > 0 else "EMPTY"
    print(f"  {t.name}: {count} rows [{status}]")

# CELL 3 — Check Silver schemas
print("\n=== Silver Layer ===")
{{#EACH silver_schemas}}
try:
    tables = spark.catalog.listTables(f"{SILVER_LH}.{{item}}")
    for t in tables:
        count = spark.table(f"{SILVER_LH}.{{item}}.{t.name}").count()
        status = "OK" if count > 0 else "EMPTY"
        print(f"  {{item}}.{t.name}: {count} rows [{status}]")
except Exception as e:
    print(f"  {{item}}: MISSING ({e})")
{{/EACH silver_schemas}}

# CELL 4 — Check Gold schemas
print("\n=== Gold Layer ===")
{{#EACH gold_schemas}}
try:
    tables = spark.catalog.listTables(f"{GOLD_LH}.{{item}}")
    for t in tables:
        count = spark.table(f"{GOLD_LH}.{{item}}.{t.name}").count()
        status = "OK" if count > 0 else "EMPTY"
        print(f"  {{item}}.{t.name}: {count} rows [{status}]")
except Exception as e:
    print(f"  {{item}}: MISSING ({e})")
{{/EACH gold_schemas}}

print("\nDiagnostic check complete.")
