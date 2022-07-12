# Databricks notebook source
v_data_source = dbutils.widgets.get("p_data_source")
v_raw_folder_path = dbutils.widgets.get("p_raw_folder_path")
v_processed_folder_path = dbutils.widgets.get("p_processed_folder_path")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

# StructType is the data to represent your rows.. StructField specifies the columns..
# If you have a nested structure you'd have structypes within StructFields.

circuits_schema = StructType(fields= [StructField("circuitId", IntegerType(), False),
                                      StructField("circuitRef", StringType(), True),
                                       StructField("name", StringType(), True),
                                        StructField("location", StringType(), True),
                                         StructField("country", StringType(), True),
                                          StructField("lat", DoubleType(), True),
                                           StructField("lng", DoubleType(), True),
                                            StructField("alt", IntegerType(), True),
                                             StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

# Read the CSV file using the spark package, set first row as header, and infer the schema.

circuits_df = spark.read.csv(f"{v_raw_folder_path}circuits.csv", 
                             header=True, 
                             schema=circuits_schema)

# COMMAND ----------

# Select and rename columns

circuits_selected_df = circuits_df.select(
    col("circuitId"), 
    col("circuitRef"), 
    col("name"), 
    col("location"), 
    col("country"), 
    col("lat").alias("latitude"),
    col("lng").alias("longitude"), 
    col("alt")
)

# can use another function here too.

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", 'circuit_id')\
.withColumnRenamed("circuitRef", 'circuit_ref')\
.withColumnRenamed("alt", 'altitude') \
.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# Create a ingestion_date timestamp

circuits_final_df = circuits_renamed_df.withColumn("env", lit("Production"))

circuits_final_df = add_ingestion_date(circuits_final_df)

# COMMAND ----------

# Write data to datalake as a parquet file overwrite if exists

circuits_final_df.write.mode("overwrite").parquet(f"{v_processed_folder_path}circuits.csv")

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

dbutils.notebook.exit("Success")
