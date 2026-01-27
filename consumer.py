from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaToSparkPipeline") \
    .getOrCreate()

kafka_df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers","localhost:9092") \
    .option("subscribe","agri-data") \
    .option("startingOffsets","earliest") \
    .load()

value_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

schema = StructType([
    StructField("location", StringType(),True),
    StructField("SOIL TYPE", StringType(),True),
    StructField("fertility", DoubleType(),True),
    StructField("LAND USE TYPE", StringType(),True),
    StructField("AVERAGE RAINFALL", DecimalType(10,2),True),
    StructField("TEMPERATURE(CELSIUS)", DecimalType(10,2),True),
    StructField("CROP SUSTAINABILITY", StringType(),True),
    StructField("season", StringType(),True),
    StructField("SATELLITE OBSERVATION DATE", StringType(),True),
    StructField("remarks", StringType(),True)
])

df = value_df.select(from_json(col("json_str"), schema).alias("data")) \
             .select("data.*")

# ---- Spark DataFrame Transformations ----
transformed_df = df \
    .withColumn("location", lower(col("location"))) \
    .withColumnRenamed("SOIL TYPE", "soil_type") \
    .withColumnRenamed("LAND USE TYPE", "land_use_type") \
    .withColumnRenamed("AVERAGE RAINFALL", "average_rainfall") \
    .withColumnRenamed("TEMPERATURE(CELSIUS)", "temperature_celsius") \
    .withColumnRenamed("CROP SUSTAINABILITY", "crop_sustainability") \
    .withColumn("satellite_observation_date", to_date(col("SATELLITE OBSERVATION DATE"), "yyyy-MM-dd")) \
    .drop("SATELLITE OBSERVATION DATE") \
    .withColumn("season", upper(col("season"))) \
    .withColumn(
        "fertility_category",
        when(col("fertility") >= 7, "HIGH")
        .when(col("fertility") >= 4, "MEDIUM")
        .otherwise("LOW")
    )

query = transformed_df.write \
    .format("csv") \
    .option("header", "true") \
    .mode("overwrite") \
    .save("/Users/mularoe/Desktop/AGRIDATA")




