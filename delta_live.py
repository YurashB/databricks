# Secrets to connect to S3
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="nyc_airbnb_scope", key="s3_access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="nyc_airbnb_scope", key="s3_secret_key"))

# Load from s3 to raw table (Bronze)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, DecimalType
import dlt

schema = nyc_schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), True),
    StructField("host_id", LongType(), True),
    StructField("host_name", StringType(), True),
    StructField("neighbourhood_group", StringType(), True),
    StructField("neighbourhood", StringType(), True),
    StructField("latitude", DecimalType(9, 6), True),
    StructField("longitude", DecimalType(9, 6), False),
    StructField("room_type", StringType(), False),
    StructField("price", IntegerType(), True),
    StructField("minimum_nights", IntegerType(), True),
    StructField("number_of_reviews", IntegerType(), True),
    StructField("last_review", StringType(), True),
    StructField("reviews_per_month", DecimalType(3, 2), True),
    StructField("calculated_host_listings_count", IntegerType(), True),
    StructField("availability_365", IntegerType(), True),
])


@dlt.table(comment="The raw data of nyc airbnb 2019 loaded from s3")
def raw_nyc_airbnb():
    return (spark
            .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .schema(schema)
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .option("quote", '"')
            .option("escape", '"')
            .load("s3a://snowflake-ab-nyc-2019/")
            )


# DLT for transformed (Silver)
import dlt
from pyspark.sql.functions import col, to_date

valid_data = {"null_id": "id is not null", "price_positive": "price > 0", "latitude_not_null": "latitude is not null",
              "longitude_not_null": "longitude is not null", "minimum_nights_not_null": "minimum_nights is not null",
              "availability_365_not_null": "availability_365 is not null"}


@dlt.table(comment="The transformed data of nyc airbnb 2019 loaded from s3")
@dlt.expect_all_or_drop(valid_data)
def transformed_nyc_airbnb():
    return (
        dlt.read_stream("raw_nyc_airbnb")
        .fillna(value="2011-03-28", subset=["last_review"])
        .fillna(0, subset=["reviews_per_month"])
        .withColumn("last_review", to_date(col("last_review")))
    )
