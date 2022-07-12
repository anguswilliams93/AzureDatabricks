# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_raw_folder_path = dbutils.widgets.get("p_raw_folder_path")
v_processed_folder_path = dbutils.widgets.get("p_processed_folder_path")

# COMMAND ----------

schema = "constructorId INT NOT NULL, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read.json(f'{v_raw_folder_path}constructors.json', schema = schema)

# COMMAND ----------

# drop unwanted columns

constructor_drop_df = constructor_df.drop(col('url'))

# add ingestion date

constructor_ingestion_df = constructor_drop_df.withColumn('ingestion_date', current_timestamp())

# rename columns

constructor_final_df = constructor_ingestion_df.select(col('constructorId').alias('constructor_id'),
                                                     col('constructorRef').alias('constructor_ref'),
                                                     col('name'),
                                                     col('nationality'),
                                                     col('ingestion_date')
                                                    )


# COMMAND ----------

constructor_final_df.write.mode('overwrite').parquet(f'{v_processed_folder_path}constructors')

# COMMAND ----------

dbutils.notebook.exit("Success")
