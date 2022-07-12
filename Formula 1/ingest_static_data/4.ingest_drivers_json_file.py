# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

v_data_source = dbutils.widgets.get("p_data_source")
v_raw_folder_path = dbutils.widgets.get("p_raw_folder_path")
v_processed_folder_path = dbutils.widgets.get("p_processed_folder_path")

# COMMAND ----------

name_schema = StructType(fields= [StructField("forename", StringType(), False),
                                      StructField("surname", StringType(), True)
                                 ]
                        )

drivers_schema = StructType(fields= [StructField("driverId", IntegerType(), False),
                                      StructField("driverRef", StringType(), True),
                                       StructField("number", IntegerType(), True),
                                        StructField("code", IntegerType(), True),
                                         StructField("name", name_schema),
                                          StructField("dob", DateType(), True),
                                           StructField("nationality", StringType(), True),
                                             StructField("url", StringType(), True)
                                     ])

drivers_raw_df  = spark.read.json(f"{v_raw_folder_path}drivers.json", 
                              schema = drivers_schema)

# COMMAND ----------

# drop unwanted columns

drivers_drop_df = drivers_raw_df.drop(col('url'))

# add ingestion date

drivers_final_df = drivers_drop_df.withColumnRenamed('driverId', 'driver_id') \
                                    .withColumnRenamed('driverRef', 'driver_ref') \
                                    .withColumn('ingestion_date', current_timestamp()) \
                                    .withColumn('name', concat(col('name.forename'), lit(" "), col('name.surname'))) \
                                    .withColumn("p_data_source", lit(v_data_source))

# COMMAND ----------

drivers_final_df.write.mode('overwrite').parquet(f'{v_processed_folder_path}drivers')

# COMMAND ----------

dbutils.notebook.exit("Success")
