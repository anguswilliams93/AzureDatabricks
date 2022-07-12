# Databricks notebook source
#import appropriate libraries

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_raw_folder_path = dbutils.widgets.get("p_raw_folder_path")
v_processed_folder_path = dbutils.widgets.get("p_processed_folder_path")

# COMMAND ----------

# MAGIC %run "../include/common_functions"

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

# StructType is the data to represent your rows.. StructField specifies the columns..
# If you have a nested structure you'd have structypes within StructFields.

schema = StructType(fields= [StructField("raceId", IntegerType(), False),
                                      StructField("year", IntegerType(), True),
                                       StructField("round", IntegerType(), True),
                                        StructField("circuitId", IntegerType(), True),
                                         StructField("name", StringType(), True),
                                          StructField("date", DateType(), True),
                                           StructField("time", StringType(), True),
                                             StructField("url", StringType(), True)
                                     ])



races_raw_df  = spark.read.csv(f"{v_raw_folder_path}races.csv", 
                             header=True,
                              schema = schema)

# COMMAND ----------

# StructType is the data to represent your rows.. StructField specifies the columns..
# If you have a nested structure you'd have structypes within StructFields.

race_with_timestamp = races_raw_df.withColumn('ingestion_date', current_timestamp()) \
                                .withColumn('race_timestamp', to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                .withColumn('p_data_source', lit(v_data_source))


# COMMAND ----------

# Merge date and time into one race_timestamp column that has a TimeStamp data type:

races_selected_df = race_with_timestamp.select(col('raceId').alias('race_id'), \
                                              col('year').alias('race_year'),
                                              col('round'),
                                              col('circuitId').alias('circuit_id'),
                                              col('name'), 
                                              col('ingestion_date'),
                                              col('race_timestamp')
                                              )

# COMMAND ----------

# Write dataframe into parquet file in processed container use partitionBy race_year

races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{v_processed_folder_path}races')

# COMMAND ----------

dbutils.notebook.exit("Success")
