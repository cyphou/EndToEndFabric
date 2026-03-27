"""Forecast generator — produces Holt-Winters forecasting notebooks and configs.

Generates:
  - NB04 Forecasting PySpark notebook (Holt-Winters + MLflow tracking)
  - forecast-config.json per industry
"""

import json
from pathlib import Path


def generate_forecast(industry_config: dict, forecast_config: dict,
                      output_dir: Path) -> list[Path]:
    """Generate forecasting artifacts for an industry demo.

    Returns list of created file paths.
    """
    industry = industry_config["industry"]
    company = industry["name"].replace(" ", "")
    artifacts = industry_config.get("fabricArtifacts", {})
    gold_lh = artifacts.get("lakehouses", {}).get("gold", "GoldLH")

    created = []

    # Write forecast config
    forecast_dir = output_dir / "Forecasting"
    forecast_dir.mkdir(parents=True, exist_ok=True)

    config_path = forecast_dir / "forecast-config.json"
    config_path.write_text(
        json.dumps(forecast_config, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    created.append(config_path)

    # Generate NB04 Forecasting notebook
    nb_path = output_dir / "notebooks" / "04_Forecasting.py"
    nb_path.parent.mkdir(parents=True, exist_ok=True)
    nb_code = _build_forecast_notebook(company, gold_lh, forecast_config)
    nb_path.write_text(nb_code, encoding="utf-8")
    created.append(nb_path)

    return created


def _build_forecast_notebook(company: str, gold_lh: str, config: dict) -> str:
    """Build the NB04 Forecasting PySpark notebook."""
    fc = config.get("forecastConfig", config)
    params = fc.get("parameters", {})
    models = fc.get("models", [])
    horizon = params.get("forecastHorizon", 6)
    confidence = params.get("confidenceLevel", 0.95)
    seasonal_periods = params.get("seasonalPeriods", 12)

    # Build model blocks
    model_blocks = []
    for i, model in enumerate(models):
        name = model["name"]
        output_table = model["outputTable"]
        output_schema = model.get("outputSchema", "analytics")
        grain_cols = model.get("grainColumns", [])
        value_col = model.get("valueColumn", "Value")
        date_col = model.get("dateColumn", "Date")
        grain_str = ", ".join(f'"{c}"' for c in grain_cols)

        model_blocks.append(f'''
# ── Model {i+1}: {name} ──────────────────────────────────────────
print(f"\\nModel {i+1}/{len(models)}: {name}")
print(f"  Output: {gold_lh}.{output_schema}.{output_table}")

try:
    # Read source data
    grain_columns = [{grain_str}]
    value_column = "{value_col}"
    date_column = "{date_col}"

    # Build forecast per grain combination
    forecast_rows = []
    source_df = spark.table(f"{gold_lh}.{output_schema}.{output_table.replace('Forecast', 'Fact')}")

    if source_df.count() == 0:
        print(f"  WARNING: No source data found, generating synthetic forecast")
        # Fallback: generate synthetic forecast data
        import datetime
        base_date = datetime.date.today().replace(day=1)
        for month_offset in range({horizon}):
            d = base_date + datetime.timedelta(days=30 * (month_offset + 1))
            forecast_rows.append(Row(
                ForecastDate=d,
                ForecastValue=float(100 + month_offset * 5),
                ConfidenceLow=float(80 + month_offset * 4),
                ConfidenceHigh=float(120 + month_offset * 6),
                Model="Synthetic",
                Grain="All",
            ))
    else:
        # Holt-Winters ExponentialSmoothing
        from pyspark.sql.functions import col, lit, date_add, current_date
        from pyspark.ml.feature import VectorAssembler
        import numpy as np

        # Aggregate to monthly grain
        monthly = source_df.groupBy(date_column).agg(
            F.sum(value_column).alias("value")
        ).orderBy(date_column).toPandas()

        if len(monthly) >= {params.get("minHistoryMonths", 12)}:
            values = monthly["value"].values.astype(float)

            # Simple Holt-Winters (additive)
            alpha, beta, gamma = 0.3, 0.1, 0.1
            n = len(values)
            s = {seasonal_periods}

            # Initialize
            level = values[0]
            trend = (values[min(s, n-1)] - values[0]) / max(s, 1)
            seasonal = [0.0] * s
            for i in range(min(s, n)):
                seasonal[i] = values[i] - level

            # Fit
            for i in range(n):
                prev_level = level
                level = alpha * (values[i] - seasonal[i % s]) + (1 - alpha) * (prev_level + trend)
                trend = beta * (level - prev_level) + (1 - beta) * trend
                seasonal[i % s] = gamma * (values[i] - level) + (1 - gamma) * seasonal[i % s]

            # Forecast
            import datetime
            last_date = monthly[date_column].max()
            if hasattr(last_date, "date"):
                last_date = last_date.date()
            elif isinstance(last_date, str):
                last_date = datetime.date.fromisoformat(last_date)

            for h in range(1, {horizon} + 1):
                fc_value = level + h * trend + seasonal[(n + h - 1) % s]
                fc_date = last_date + datetime.timedelta(days=30 * h)
                residual_std = float(np.std(values[-min(12, n):]))
                z = 1.96  # ~95% confidence
                forecast_rows.append(Row(
                    ForecastDate=fc_date,
                    ForecastValue=round(fc_value, 2),
                    ConfidenceLow=round(fc_value - z * residual_std, 2),
                    ConfidenceHigh=round(fc_value + z * residual_std, 2),
                    Model="HoltWinters",
                    Grain="All",
                ))
        else:
            print(f"  WARNING: Insufficient history ({{len(monthly)}} months), using naive forecast")
            avg_val = float(monthly["value"].mean())
            import datetime
            last_date = monthly[date_column].max()
            if hasattr(last_date, "date"):
                last_date = last_date.date()
            elif isinstance(last_date, str):
                last_date = datetime.date.fromisoformat(last_date)
            for h in range(1, {horizon} + 1):
                fc_date = last_date + datetime.timedelta(days=30 * h)
                forecast_rows.append(Row(
                    ForecastDate=fc_date,
                    ForecastValue=round(avg_val, 2),
                    ConfidenceLow=round(avg_val * 0.85, 2),
                    ConfidenceHigh=round(avg_val * 1.15, 2),
                    Model="Naive",
                    Grain="All",
                ))

    # Write forecast table
    if forecast_rows:
        df_fc = spark.createDataFrame(forecast_rows)
        df_fc.write.mode("overwrite").format("delta") \\
            .option("overwriteSchema", "true") \\
            .saveAsTable(f"{gold_lh}.{output_schema}.{output_table}")
        print(f"  ✓ {output_table}: {{len(forecast_rows)}} forecast rows")

    # MLflow tracking
    with mlflow.start_run(run_name="{name}", nested=True):
        mlflow.log_param("model", "{name}")
        mlflow.log_param("horizon", {horizon})
        mlflow.log_param("grain_columns", "{', '.join(grain_cols)}")
        mlflow.log_metric("forecast_rows", len(forecast_rows))

except Exception as e:
    print(f"  ERROR in {name}: {{e}}")
    errors.append("{name}")
''')

    all_model_blocks = "\n".join(model_blocks)

    return f'''# Fabric Notebook
# {company} — NB04: Forecasting
# Holt-Winters ExponentialSmoothing with MLflow tracking.
# Models: {", ".join(m["name"] for m in models)}

# CELL 1 — Setup
import mlflow
from pyspark.sql import Row
from pyspark.sql import functions as F
import datetime

GOLD_LH = "{gold_lh}"
FORECAST_HORIZON = {horizon}
CONFIDENCE_LEVEL = {confidence}
SEASONAL_PERIODS = {seasonal_periods}

# Create analytics schema
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {gold_lh}.analytics")

errors = []
print("=" * 60)
print(f"  {company} Forecasting — {{len([{', '.join(repr(m['name']) for m in models)}])}} models")
print("=" * 60)

# Start MLflow parent run
mlflow.set_experiment(f"/Shared/{company}_Forecasting")
with mlflow.start_run(run_name=f"NB04_Forecasting_{{datetime.datetime.now().strftime('%Y%m%d_%H%M')}}"):
    mlflow.log_param("company", "{company}")
    mlflow.log_param("horizon", FORECAST_HORIZON)
    mlflow.log_param("models", {len(models)})

{all_model_blocks}

# CELL FINAL — Summary
print("\\n" + "=" * 60)
if errors:
    print(f"  Forecasting complete with {{len(errors)}} error(s): {{', '.join(errors)}}")
else:
    print(f"  Forecasting complete — all {len(models)} models succeeded")
print("=" * 60)
'''
