# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS wtr

# COMMAND ----------

# MAGIC %sql
# MAGIC USE wtr

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS water_withdrawal_analysis USING DELTA LOCATION 'presentation_folder_path'

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES;
