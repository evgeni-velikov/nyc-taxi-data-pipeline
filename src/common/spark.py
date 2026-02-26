import os

from pyspark.sql import SparkSession


access_key = os.environ.get("AWS_ACCESS_KEY_ID", "YOUR_KEY")
secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "YOUR_SECRET")

def get_spark_session(app_name: str = "nyc-taxi") -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("spark://spark-master:7077")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .enableHiveSupport()
    )

    spark = builder.getOrCreate()
    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.s3a.access.key", access_key)
    hadoop_conf.set("fs.s3a.secret.key", secret_key)
    hadoop_conf.set("fs.s3a.endpoint", "s3.amazonaws.com")

    return spark
