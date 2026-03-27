# Fabric Notebook
# {{company_name}} — NB02: Web Enrichment
# Fetches external API data and writes to {{silver_lh}}.web schema.

# CELL 1 — Configuration
SILVER_LH = "{{silver_lh}}"

# CELL 2 — Create web schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_LH}.web")
print(f"Schema ready: {SILVER_LH}.web")

# CELL 3 — Web enrichment
# Extend per industry: exchange rates, weather data, metadata, etc.
import json
from pyspark.sql import Row

web_data = [
    Row(SourceName="ExchangeRates", LastRefresh="2024-01-01", Status="OK"),
    Row(SourceName="IndustryData", LastRefresh="2024-01-01", Status="OK"),
]
df_web = spark.createDataFrame(web_data)
df_web.write.mode("overwrite").format("delta") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_LH}.web.WebSources")
print(f"Web enrichment: {len(web_data)} sources logged")
