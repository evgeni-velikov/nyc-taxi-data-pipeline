import os
import logging
import argparse
from pyspark.sql import DataFrame

from src.common.spark import get_spark_session


logger = logging.getLogger(__name__)

def export_to_snowflake(df: DataFrame, table_name: str, cluster_by: list[str] | None = None) -> None:
    logger.info(f"Starting export to Snowflake: {table_name.upper()}")
    snowflake_options = {
        "sfURL": f"{os.environ.get('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
        "sfAccount": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "sfUser": os.environ.get("SNOWFLAKE_USER"),
        "sfPassword": os.environ.get("SNOWFLAKE_PASSWORD"),
        "sfDatabase": os.environ.get("SNOWFLAKE_DATABASE"),
        "sfWarehouse": os.environ.get("SNOWFLAKE_WAREHOUSE"),
        "sfSchema": os.environ.get("SNOWFLAKE_SCHEMA", "NYC_TAXI"),
        "sfRole": os.environ.get("SNOWFLAKE_ROLE"),
    }

    df.write \
        .format("snowflake") \
        .options(**snowflake_options) \
        .option("dbtable", table_name.upper()) \
        .option("sfDatabase", os.environ.get("SNOWFLAKE_DATABASE")) \
        .option("sfSchema", os.environ.get("SNOWFLAKE_SCHEMA", "NYC_TAXI")) \
        .mode("overwrite") \
        .save()
    logger.info(f"Export complete: {table_name.upper()}")

    if cluster_by:
        columns = ", ".join(cluster_by)
        logger.info(f"Applying clustering on: {columns}")
        alter_sql = f"ALTER TABLE {table_name.upper()} CLUSTER BY ({columns})"
        df.sparkSession.sparkContext._jvm.net.snowflake.spark.snowflake \
            .Utils.runQuery(snowflake_options, alter_sql)
        logger.info("Clustering applied successfully")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--source_table", required=True)
    parser.add_argument("--target_table", required=True)
    parser.add_argument("--cluster_by", nargs="+", default=[])
    args = parser.parse_args()

    spark = get_spark_session()
    df = spark.table(args.source_table)
    export_to_snowflake(df=df, table_name=args.target_table, cluster_by=args.cluster_by)
