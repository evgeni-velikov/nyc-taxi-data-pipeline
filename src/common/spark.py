import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

from src.common.environment import require_env


def get_spark_session(app_name: str = "nyc-taxi") -> SparkSession:
    access_key = require_env("AWS_ACCESS_KEY_ID")
    secret_key = require_env("AWS_SECRET_ACCESS_KEY")

    master = os.environ.get("SPARK_MASTER", "local[*]")
    s3_endpoint = os.environ.get("S3_ENDPOINT", "s3.amazonaws.com")

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master)

        # Hive
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")

        # Delta
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # Warehouse
        .config("spark.sql.warehouse.dir", "/warehouse")

        # S3
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    return spark
