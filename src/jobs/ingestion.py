from datetime import datetime
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F

from src.common.utils import read_file, normalize_schema, enrich_with_metadata
from src.common.config import Config


def run_ingestion(spark: SparkSession, config: Config):
    for table in config.bronze_tables:
        bronze_table = f"{table}_{config.suffix_table}"
        next_partition = _get_next_partition(spark=spark, table=bronze_table, fallback_date=config.partition_date)

        date_file_path = f"{next_partition.month:02d}_{next_partition.year}"
        file_path = (
            f"{config.raw_folder}/"
            f"{config.incremental_folder}/"
            f"{next_partition.year}/"
            f"{table}_{date_file_path}."
            f"{config.raw_file_format}"
        )

        reference_schema = spark.table(bronze_table).schema
        df = read_file(spark, file_path, config.raw_file_format)
        df = normalize_schema(df=df, reference_schema=reference_schema)
        df = enrich_with_metadata(df=df, partition_date=next_partition)

        (
            df.write
            .format("delta").mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(bronze_table)
        )


def _get_next_partition(spark, table, fallback_date):
    latest_partition = (
        spark.sql(f"SHOW PARTITIONS {table}")
        .selectExpr("split(partition, '=')[1] as partition_date")
        .agg(F.max("partition_date").alias("max_date"))
        .collect()[0][0]
    )

    if not latest_partition:
        return fallback_date

    latest_dt = datetime.strptime(latest_partition, "%Y-%m-%d")
    return latest_dt + relativedelta(months=1)
