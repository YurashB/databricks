# Databricks practical task

For this task I used two ways to create ETL in databricks. The first one is to use Delta Table/Jobs/Autoloader and the
second one is to use just Delta Live Table. Code for Delta Table/Jobs/Autoloader is in ```delta.py``` and for Delta Live Table in```delta_live.py```

## Delta Table/Jobs/Autoloader

### 1. Create Cluster and Notebook

Create Cluster and Notebook to use Databricks.

### 2. Create database

To create new database run

```sql
%sql
CREATE
DATABASE nyc_airbnb;
```

### 3. Create table for data

Table for raw data

```sql
%sql
CREATE
OR REPLACE TABLE nyc_airbnb.raw_nyc_airbnb_table (
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
```

Table for transformed data

```sql
%sql
CREATE
OR REPLACE TABLE nyc_airbnb.transformed_nyc_airbnb_table (
    id                             LONG NOT NULL PRIMARY KEY,
    name                           STRING,
    host_id                        LONG,
    host_name                      STRING,
    neighbourhood_group            STRING,
    neighbourhood                  STRING,
    latitude                       DECIMAL(9, 6) NOT NULL,
    longitude                      DECIMAL(9, 6) NOT NULL,
    room_type                      STRING,
    price                          INTEGER,
    minimum_nights                 INTEGER NOT NULL,
    number_of_reviews              INTEGER,
    last_review                    DATE,
    reviews_per_month              DECIMAL(3, 2),
    calculated_host_listings_count INTEGER,
    availability_365               INTEGER NOT NULL  
)
USING DELTA;

ALTER TABLE nyc_airbnb.transformed_nyc_airbnb_table
    ADD CONSTRAINT priceUnderZero CHECK (price > 0);
```

### Crate s3 bucket with proper aim user and policy

1. First of all, you need to create s3 bucket on aws
2. After creating, you need to create new policy with

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:GetObjectVersion"
      ],
      "Resource": "arn:aws:s3:::snowflake-ab-nyc-2019/*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": "arn:aws:s3:::snowflake-ab-nyc-2019",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "*"
          ]
        }
      }
    }
  ]
}
```

3. Create IAM user and add created policy to user
4. After that create new access key
5. Save this key to use in next steps

### 4. Create notebook for job

On this step, you need to create notebook with some python scripts that will be used for job.
This script use autoloader for steaming data.

1. First of all you need to add secrets to your cluster that will be used for job. Open clusters CLI and create scope

```bash
databricks secrets create-scope <scope-name>
```

2. Create secrets with access and secret key to your s3 bucket

``` bash
databricks secrets put-secret --json '{
  "scope": "<scope-name>",
  "key": "s3_access_key", 
  "string_value": "<provided-access-key>"
}'
```

and

```bash
databricks secrets put-secret --json '{
  "scope": "<scope-name>",
  "key": "s3_secret_key",
  "string_value": "<provided-secret-key>"
}'
```

3. Create notebook with code

```python
# Secrets to connect to S3
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="<scope-name>", key="s3_access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="<scope-name>", key="s3_secret_key"))
```

```python
# List files if successfully connected 
dbutils.fs.ls(f"s3a://snowflake-ab-nyc-2019/")  # your s3 path
```

```python
# Load from s3 to raw table (Bronze)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, DecimalType

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
```

```python
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
```

4. Create a job to schedule process. You can use json create job. Remember, this is my example. You can easily create
   your job via UI

```json
{
  "name": "<your-job-name>",
  "description": "Etl for load data from s3 -> save it Bronze table -> transform data -> save in Silver",
  "email_notifications": {
    "on_failure": [
      "<email-to-send-message>"
    ],
    "no_alert_for_skipped_runs": false
  },
  "webhook_notifications": {},
  "notification_settings": {
    "no_alert_for_skipped_runs": false,
    "no_alert_for_canceled_runs": false
  },
  "timeout_seconds": 0,
  "schedule": {
    // Scheduled every day at 2 am UTC
    "quartz_cron_expression": "26 0 2 * * ?",
    "timezone_id": "UTC",
    "pause_status": "UNPAUSED"
  },
  "max_concurrent_runs": 1,
  "tasks": [
    {
      "task_key": "nyc_airbnb_etl",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Workspace/Users/yurash.bohdan@lll.kpi.ua/spark_etl",
        // path to notebook with autoloader
        "source": "WORKSPACE"
      },
      "existing_cluster_id": "0930-095750-11b8avqn",
      "timeout_seconds": 0,
      "email_notifications": {},
      "notification_settings": {
        "no_alert_for_skipped_runs": false,
        "no_alert_for_canceled_runs": false,
        "alert_on_last_attempt": false
      },
      "webhook_notifications": {}
    }
  ],
  "queue": {
    "enabled": true
  },
  "run_as": {
    "user_name": "<your-username>"
  }
}
```

## Delta Live Table

### 1. Create S3 policy

You need to get access and secret key as in the previous example

### 2. Create Notebook

Create Notebook with code below

```python
# Secrets to connect to S3
spark.conf.set("fs.s3a.access.key", dbutils.secrets.get(scope="nyc_airbnb_scope", key="s3_access_key"))
spark.conf.set("fs.s3a.secret.key", dbutils.secrets.get(scope="nyc_airbnb_scope", key="s3_secret_key"))
```

```python
# Load from s3 to raw table (Bronze)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, DecimalType
import dlt

# Schema for table
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


# DLT
@dlt.table(comment="The raw data of nyc airbnb 2019 loaded from s3")
def raw_nyc_airbnb():
    try:
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
    except Exception as ex:
        print("Error Class       : " + ex.getErrorClass())
        print("Message parameters: " + str(ex.getMessageParameters()))
        print(ex)
```

```python
# Transform and filter data (Silver)
import dlt
from pyspark.sql.functions import col, to_date

# Drop all invalid values
valid_data = {"null_id": "id is not null", "price_positive": "price > 0", "latitude_not_null": "latitude is not null",
              "longitude_not_null": "longitude is not null", "minimum_nights_not_null": "minimum_nights is not null",
              "availability_365_not_null": "availability_365 is not null"}


# DLT with transformation
@dlt.table(comment="The transformed data of nyc airbnb 2019 loaded from s3")
@dlt.expect_all_or_drop(valid_data)
def transformed_nyc_airbnb():
    return (
        dlt.read_stream("raw_nyc_airbnb")
        .fillna(value="2011-03-28", subset=["last_review"])
        .fillna(0, subset=["reviews_per_month"])
        .withColumn("last_review", to_date(col("last_review")))
    )
```

### 3. Create Delta Live Table pipline

To create Delta live table pipline you need to use UI. For my case json settings is provided. After creating don`t
forget to schedule pipline

```json
{
  "id": "771f5c8b-22d5-4227-ba70-fbce1ef85380",
  "pipeline_type": "WORKSPACE",
  "clusters": [
    {
      "label": "default",
      "num_workers": 1
    }
  ],
  "development": true,
  "notifications": [
    {
      "email_recipients": [
        "example@example.com"
      ],
      "alerts": [
        "on-update-failure",
        "on-update-fatal-failure",
        "on-flow-failure"
      ]
    }
  ],
  "continuous": true,
  "channel": "CURRENT",
  "photon": false,
  "libraries": [
    {
      "notebook": {
        "path": "/Users/yurash.bohdan@lll.kpi.ua/Delta live tables"
      }
    }
  ],
  "name": "delta live table etl",
  "edition": "ADVANCED",
  "storage": "dbfs:/pipelines/771f5c8b-22d5-4227-ba70-fbce1ef85380",
  "data_sampling": false
}
```