# SQL commands to create tables and database
"""
%sql
CREATE IF NOT EXIST DATABASE nyc_airbnb;

%sql
CREATE OR REPLACE TABLE nyc_airbnb.raw_nyc_airbnb_table (
    id                             LONG,
    name                           STRING,
    host_id                        LONG,
    host_name                      STRING,
    neighbourhood_group            STRING,
    neighbourhood                  STRING,
    latitude                       DECIMAL(9, 6),
    longitude                      DECIMAL(9, 6),
    room_type                      STRING,
    price                          INTEGER,
    minimum_nights                 INTEGER,
    number_of_reviews              INTEGER,
    last_review                    STRING,
    reviews_per_month              DECIMAL(3, 2),
    calculated_host_listings_count INTEGER,
    availability_365               INTEGER
)
USING DELTA

%sql
CREATE OR REPLACE TABLE nyc_airbnb.raw_nyc_airbnb_table (
    id                             LONG,
    name                           STRING,
    host_id                        LONG,
    host_name                      STRING,
    neighbourhood_group            STRING,
    neighbourhood                  STRING,
    latitude                       DECIMAL(9, 6),
    longitude                      DECIMAL(9, 6),
    room_type                      STRING,
    price                          INTEGER,
    minimum_nights                 INTEGER,
    number_of_reviews              INTEGER,
    last_review                    STRING,
    reviews_per_month              DECIMAL(3, 2),
    calculated_host_listings_count INTEGER,
    availability_365               INTEGER
)
USING DELTA
"""

# Secrets to connect to S3
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="nyc_airbnb_scope", key="s3_access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="nyc_airbnb_scope", key="s3_secret_key"))

# List files if successfully connected
dbutils.fs.ls(f"s3a://snowflake-ab-nyc-2019/")

# Load from s3 to raw table (Bronze)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, DecimalType
schema = nyc_schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("host_id", LongType(), True),
    StructField("host_name", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", DecimalType(9,6), True),
    StructField("longitude", DecimalType(9,6), False),
    StructField("room_type", StringType(), False),
    StructField("price", IntegerType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("last_review", StringType(), True),
    StructField("reviews_per_month", DecimalType(3,2), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
])

try:
    (spark
        .readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", "/mnt/data/schema")
        .schema(schema)
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .option("checkpointLocation", "/mnt/data/checkpoints/raw")
        .option("quote", '"')
        .option("escape", '"')
        .load("s3a://snowflake-ab-nyc-2019/")
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", "/mnt/data/checkpoints/raw")
        .toTable("nyc_airbnb.raw_nyc_airbnb_table")
    )
except Exception as ex:
    print("Error Class       : " + ex.getErrorClass())
    print("Message parameters: " + str(ex.getMessageParameters()))
    print(ex)

# Load from raw table to transformed table (Silver)
from pyspark.sql.functions import col, to_date

try:
    (spark
        .readStream
        .table("nyc_airbnb.raw_nyc_airbnb_table")
        .filter("id is not null")
        .filter("price > 0")
        .fillna(value="2011-03-28", subset=["last_review"])
        .fillna(0, subset=["reviews_per_month"])
        .filter(col("latitude").isNotNull() & col("longitude").isNotNull())
        .filter(col("minimum_nights").isNotNull() & col("availability_365").isNotNull())
        .withColumn("last_review", to_date(col("last_review")))
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", "/mnt/data/checkpoints/transformed")
        .toTable("nyc_airbnb.transformed_nyc_airbnb_table")
    )
except Exception as ex:
    print("Error Class       : " + ex.getErrorClass())
    print("Message parameters: " + str(ex.getMessageParameters()))
    print(ex)
