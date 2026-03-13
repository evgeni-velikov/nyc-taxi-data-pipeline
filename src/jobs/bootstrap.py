import logging
from pyspark.sql import SparkSession

from src.common.config import Config
from src.common.utils import read_file, normalize_schema, enrich_with_metadata


logger = logging.getLogger(__name__)

def run_bootstrap(spark: SparkSession, config: Config):
    logger.info(f"Starting bootstrap for schema: {config.bronze_schema}")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {config.bronze_schema}")
    spark.sql(f"USE SCHEMA {config.bronze_schema}")

    for table in config.bronze_tables:
        bronze_table = f"{config.catalog_name}.{config.bronze_schema}.{table}_{config.suffix_table}"

        file_path = (
            f"{config.raw_folder}/"
            f"{table}_{config.bootstrap_entry_point}."
            f"{config.raw_file_format}"
        )

        logger.info(f"Processing table: {bronze_table}")
        df = read_file(spark, file_path, config.raw_file_format)
        df = normalize_schema(df)
        df = enrich_with_metadata(df, partition_date=config.partition_date)

        (
            df.write.format("delta")
            .partitionBy("partition_date")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(bronze_table)
        )
        logger.info(f"Successfully written: {bronze_table}")

    logger.info(f"Bootstrap complete. Processed {len(config.bronze_tables)} tables.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from src.common.spark import get_spark_session
    run_bootstrap(spark=get_spark_session(), config=Config())
