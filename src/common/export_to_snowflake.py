import os
from pyspark.sql import DataFrame


def export_to_snowflake(df: DataFrame, table_name: str, cluster_by: list[str] | None = None) -> None:
    snowflake_options = {
        "sfURL": f"{os.environ.get('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfUser": os.environ.get("SNOWFLAKE_USER"),
        "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "MARTS"),
        "sfRole": os.environ.get("SNOWFLAKE_ROLE"),
    }

    df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table_name.upper()) \
        .mode("overwrite") \
        .save()

    if cluster_by:
        columns = ", ".join(cluster_by)
        alter_sql = f"ALTER TABLE {table_name.upper()} CLUSTER BY ({columns})"
        df.sparkSession.sparkContext._jvm.net.snowflake.spark.snowflake \
            .Utils.runQuery(snowflake_options, alter_sql)


# from common.snowflake import export_to_snowflake

# spark = get_spark_session()
# df = spark.table("gold.marts_trips_charges_hourly")
# export_to_snowflake(
#     df=df,
#     table_name="MARTS_TRIPS_CHARGES_HOURLY",
#     cluster_by=["date", "pickup_location_id", "dropoff_location_id"]
# )
