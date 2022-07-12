# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ###Access Dataframes using SparkSQL
# MAGIC 
# MAGIC * Create temporary views
# MAGIC * Create global views
# MAGIC * Access via SQL Command & Python Command

# COMMAND ----------

# MAGIC %run "../include/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}race_results")

# COMMAND ----------

race_results_df.createTempView("tmp_vw_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM tmp_vw_race_results 
# MAGIC WHERE race_year = '2019'

# COMMAND ----------

race_results_2020_df = spark.sql('SELECT * FROM tmp_vw_race_results WHERE race_year = 2020')

# COMMAND ----------

race_results_2020_df.createOrReplaceGlobalTempView("g_vw_race_results_2020")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SHOW TABLES in global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM global_temp.g_vw_race_results_2020
