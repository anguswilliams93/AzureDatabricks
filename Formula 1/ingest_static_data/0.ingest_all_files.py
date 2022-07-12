# Databricks notebook source
# MAGIC %run "/Users/gusswilliams@gmail.com/Formula 1/include/configuration"

# COMMAND ----------

p_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_result = dbutils.notebook.run("1.ingest_circuits_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("2.ingest_races_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("3.ingest_constructors_json_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("4.ingest_drivers_json_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("5.ingest_results_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("6.ingest_pit_stops_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("7.ingest_lap_times_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})

# COMMAND ----------

v_result = dbutils.notebook.run("8.ingest_qualifying_file", 0, {"p_data_source": p_data_source, "p_raw_folder_path": raw_folder_path, "p_processed_folder_path": processed_folder_path})
