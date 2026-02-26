from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import NullType


def read_file(spark: SparkSession, path: str, file_format: str):
    """
    Reads a file using Spark.
    Works both locally and in Databricks.
    """

    # Try Databricks file check first
    try:
        dbutils.fs.ls(path)  # type: ignore
    except NameError:
        # Not in Databricks, skip dbutils check
        pass
    except Exception as e:
        raise FileNotFoundError(f"Path not found: {path}") from e

    return spark.read.format(file_format).load(path)


def normalize_schema(df, reference_schema=None):
    reference_fields = {}
    if reference_schema:
        reference_fields = {f.name: f.dataType for f in reference_schema.fields}

    expressions = []
    for field in df.schema.fields:
        col_name = field.name

        if col_name in reference_fields:
            expressions.append(
                F.col(col_name).cast(reference_fields[col_name]).alias(col_name)
            )
        elif isinstance(field.dataType, NullType):
            expressions.append(
                F.col(col_name).cast("string").alias(col_name)
            )
        else:
            expressions.append(F.col(col_name))

    return df.select(*expressions)


def enrich_with_metadata(df, partition_date):
    return (
        df.withColumn("processing_time", F.current_timestamp())
          .withColumn("partition_date", F.lit(partition_date).cast("date"))
    )


def is_databricks() -> bool:
    """
    Detect if running inside Databricks.
    """
    try:
        import dbruntime  # type: ignore

        # Spark is global so if is databricks can used it
        spark.sql("CREATE CATALOG IF NOT EXISTS 'nyc-taxi';")
        spark.sql("USE CATALOG 'nyc-taxi';")

        return True
    except ImportError:
        return False


def get_or_create_spark():
    if is_databricks():
        # Spark already exists in Databricks
        return spark  # noqa
    else:
        from src.common.spark import get_spark_session
        return get_spark_session("nyc-taxi")
