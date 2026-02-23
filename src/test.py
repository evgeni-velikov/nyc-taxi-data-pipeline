from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import current_date

spark = (
    SparkSession.builder
    .appName("bronze-test")
    .enableHiveSupport()
    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

data = [
    Row(vendor_id=1, passenger_count=2, trip_distance=3.5),
    Row(vendor_id=2, passenger_count=1, trip_distance=7.8),
]

df = spark.createDataFrame(data)

df = df.withColumn("ingestion_date", current_date())

(
    df.write
    .mode("append")
    .partitionBy("ingestion_date")
    .saveAsTable("bronze.yellow_trip_data")
)

spark.stop()

# docker compose exec spark-master spark-submit /app/src/test.py
# docker compose exec spark-thrift spark-sql
# docker compose run --rm dbt dbt debug
# docker compose run --rm dbt dbt run
