from pyspark.sql import SparkSession

from src.common.config import Config
from src.common.utils import read_file, normalize_schema, enrich_with_metadata


def run_bootstrap(spark: SparkSession, config: Config):
    spark.sql(f"USE SCHEMA {config.bronze_schema}")

    for table in config.bronze_tables:
        bronze_table = f"{config.catalog_name}.{config.bronze_schema}.{table}_{config.suffix_table}"

        file_path = (
            f"{config.raw_folder}/"
            f"{table}_{config.bootstrap_entry_point}."
            f"{config.raw_file_format}"
        )

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


if __name__ == "__main__":
    from src.common.spark import get_spark_session
    run_bootstrap(spark=get_spark_session(), config=Config())
