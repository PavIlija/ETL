# Databricks notebook source
# MAGIC %md ## Exctract data from API
# MAGIC

# COMMAND ----------

import requests
import pandas as pd
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_weather_data(api_url, api_key, city):
    try:
        response = requests.get(f"{api_url}?q={city}&appid={api_key}")
        response.raise_for_status()  # Raises HTTPError for bad responses
        data = response.json()
        
        if "main" in data and "weather" in data:
            weather_data = {
                "city": city,
                "temperature": data["main"]["temp"],
                "humidity": data["main"]["humidity"],
                "weather": data["weather"][0]["description"],
                "timestamp": datetime.now()
            }
            return pd.DataFrame([weather_data])
        else:
            logger.error("Unexpected response structure: %s", data)
            return pd.DataFrame()  # Return an empty DataFrame
    except requests.exceptions.RequestException as e:
        logger.error("Request failed: %s", e)
        return pd.DataFrame()  # Return an empty DataFrame

# Example API URL and key
api_url = "http://api.openweathermap.org/data/2.5/weather"
api_key = "your_api_key_here"
city = "London"
raw_data_df = extract_weather_data(api_url, api_key, city)


# COMMAND ----------

# MAGIC %md ##  Load Data into Redshift Staging Table

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql import DataFrame

# Initialize Spark session
spark = SparkSession.builder.appName("ETL_Pipeline_Weather").getOrCreate()

# Define schema
schema = StructType([
    StructField("city", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("weather", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Convert Pandas DataFrame to Spark DataFrame
if not raw_data_df.empty:
    staging_df = spark.createDataFrame(raw_data_df, schema=schema)
else:
    logger.warning("No data extracted to load into staging table.")


# COMMAND ----------

# MAGIC %md ## Validate Data (NULL and Duplicate Checks)
# MAGIC

# COMMAND ----------

# NULL validation
validated_df = staging_df.dropna()

# Duplicate validation (based on city and timestamp)
validated_df = validated_df.dropDuplicates(["city", "timestamp"])

# Write validated data to Redshift staging table
try:
    redshift_url = "jdbc:redshift://your_redshift_cluster_endpoint:5439/your_database"
    redshift_properties = {
        "user": "your_username",
        "password": "your_password",
        "driver": "com.amazon.redshift.jdbc.Driver"
    }
    
    validated_df.write \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", "staging_weather_data") \
        .option("user", redshift_properties["user"]) \
        .option("password", redshift_properties["password"]) \
        .option("driver", redshift_properties["driver"]) \
        .mode("overwrite") \
        .save()
except Exception as e:
    logger.error("Error loading data into Redshift staging table: %s", e)


# COMMAND ----------

# MAGIC %md ### Incremental Load into Redshift Final Table

# COMMAND ----------

# Load validated data from Redshift staging table
try:
    validated_df = spark.read \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", "staging_weather_data") \
        .option("user", redshift_properties["user"]) \
        .option("password", redshift_properties["password"]) \
        .option("driver", redshift_properties["driver"]) \
        .load()

    # Get the latest timestamp from the final table
    latest_timestamp_query = "(SELECT MAX(timestamp) AS max_ts FROM weather_data)"
    latest_timestamp_df = spark.read \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", latest_timestamp_query) \
        .option("user", redshift_properties["user"]) \
        .option("password", redshift_properties["password"]) \
        .option("driver", redshift_properties["driver"]) \
        .load()

    latest_timestamp = latest_timestamp_df.collect()[0]["max_ts"]

    # Filter new data based on the latest timestamp
    if latest_timestamp:
        new_data_df = validated_df.filter(validated_df.timestamp > latest_timestamp)
    else:
        new_data_df = validated_df

    # Append new data to the final table
    new_data_df.write \
        .format("jdbc") \
        .option("url", redshift_url) \
        .option("dbtable", "weather_data") \
        .option("user", redshift_properties["user"]) \
        .option("password", redshift_properties["password"]) \
        .option("driver", redshift_properties["driver"]) \
        .mode("append") \
        .save()
except Exception as e:
    logger.error("Error during incremental load to Redshift: %s", e)


# COMMAND ----------


