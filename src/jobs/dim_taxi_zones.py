from pyspark.sql import SparkSession

from src.common.config import Config
from src.common.utils import read_file


def get_taxi_zones(spark: SparkSession, config: Config):
    file_path = f"{config.raw_folder}/taxi_zones.csv"
    df = read_file(
        spark=spark, path=file_path, file_format="csv",
        options={"header": "true", "inferSchema": "true"}
    )
    table = f"{config.catalog_name}.{config.gold_schema}.taxi_trip_zones"

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table)
    )


if __name__ == "__main__":
    from src.common.spark import get_spark_session
    get_taxi_zones(spark=get_spark_session(), config=Config())
