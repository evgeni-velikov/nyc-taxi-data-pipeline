import logging
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession, functions as F

from src.common.utils import read_file, normalize_schema, enrich_with_metadata
from src.common.config import Config


logger = logging.getLogger(__name__)

def run_ingestion(spark: SparkSession, config: Config):
    logger.info(f"Starting ingestion for {len(config.bronze_tables)} tables.")

    for table in config.bronze_tables:
        bronze_table = f"{config.bronze_schema}.{table}_{config.suffix_table}"
        next_partition = _get_next_partition(spark=spark, table=bronze_table, fallback_date=config.partition_date)

        date_file_path = f"{next_partition.month:02d}_{next_partition.year}"
        file_path = (
            f"{config.incremental_folder}/"
            f"{next_partition.year}/"
            f"{table}_{date_file_path}."
            f"{config.raw_file_format}"
        )

        logger.info(f"Ingesting {bronze_table} for partition: {next_partition.date()}")
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
        logger.info(f"Successfully appended partition {next_partition.date()} to {bronze_table}.")

    logger.info("Ingestion complete.")


def _get_next_partition(spark, table, fallback_date):
    result = (
        spark.table(table)
        .agg(F.max("partition_date"))
        .collect()
    )

    latest_partition = result[0][0]
    if not latest_partition:
        logger.info(f"No existing partitions found for {table}, using fallback date: {fallback_date}")
        return fallback_date

    next_partition = latest_partition + relativedelta(months=1)
    logger.info(f"Latest partition for {table}: {latest_partition}, next: {next_partition.date()}")
    return next_partition


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.common.spark import get_spark_session
    run_ingestion(spark=get_spark_session(), config=Config())
