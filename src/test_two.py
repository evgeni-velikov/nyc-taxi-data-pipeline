from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("test-dim-date")
    .master("spark://spark-master:7077")
    .config("spark.sql.catalogImplementation", "hive")
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.table("gold.dim_date_calendar")

df.show(10, truncate=False)

spark.stop()
