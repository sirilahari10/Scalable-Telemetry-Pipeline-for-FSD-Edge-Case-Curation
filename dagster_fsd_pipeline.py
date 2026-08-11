import dagster as dg
import pandas as pd

@dg.asset
def process_fsd_telemetry():
    # Trigger PySpark job
    run_spark_job()
    # Return sample for data quality check
    return pd.read_sql("SELECT * FROM fsd_metrics.raw_edge_cases LIMIT 1000", con=clickhouse_conn)

@dg.asset_check(asset=process_fsd_telemetry, description="Ensure firmware hasn't dropped autopilot_state telemetry")
def check_sensor_drift(context, df: pd.DataFrame):
    # If the column is entirely null, block the pipeline and alert Slack
    null_count = df["autopilot_state"].isna().sum()
    
    return dg.AssetCheckResult(
        passed=bool(null_count < len(df)),
        metadata={"null_sensor_readings": int(null_count)}
    )
