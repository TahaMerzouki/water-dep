# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

table_location = f"{presentation_folder_path}/water_withdrawl_analysis/withdrawl_analysis"

# COMMAND ----------

# Create database with location poitning to processed folder if it doesn't exist
spark.sql(f"CREATE DATABASE IF NOT EXISTS wtr_processed LOCATION '{processed_folder_path}'")

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS wtr_presentation LOCATION '{presentation_folder_path}'")

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC DATABASE wtr_processed

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()
