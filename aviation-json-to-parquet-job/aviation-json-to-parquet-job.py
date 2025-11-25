from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
import sys

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

try:
    df = spark.read.option("multiLine", True).json("s3://aviation-flights-raw-els9cy/")

    # Flatten nested JSON
    df = df.select(F.explode("data").alias("flight")).select("flight.*")

    df_flat = df.select(
        "flight_date",
        "flight_status",
        F.col("flight.number").alias("flight_number"),
        F.col("airline.name").alias("airline_name"),
        F.col("departure.airport").alias("dep_airport"),
        F.col("arrival.airport").alias("arr_airport")
    )

    df_flat.write.mode("overwrite").parquet("s3://aviation-flights-converted-els9cy/")

    job.commit()
except Exception as e:
    print("ETL Error:", e)
    job.commit()
