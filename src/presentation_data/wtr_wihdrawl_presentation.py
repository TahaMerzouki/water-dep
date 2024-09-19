# Databricks notebook source
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
sys.path.append("/Workspace/Repos/Water-project/water-dep/src/includes")

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/water_withdrawl_perc_partitioned", header=True)

# COMMAND ----------

# Add a timestamp for when the data was loaded into the presentation layer
df_with_timestamp = df.withColumn("loaded_at", F.current_timestamp())

# COMMAND ----------

display(df_with_timestamp)

# COMMAND ----------

df_optimized = df_with_timestamp \
    .withColumn("Year", F.col("Year").cast("int")) \
    .withColumn("Total water withdrawal", F.col("Total water withdrawal").cast("double")) \
    .withColumn("Agricultural water withdrawal", F.col("Agricultural water withdrawal").cast("double")) \
    .withColumn("Industrial water withdrawal", F.col("Industrial water withdrawal").cast("double")) \
    .withColumn("Municipal water withdrawal", F.col("Municipal water withdrawal").cast("double")) \
    .withColumn("agricultural_percentage", F.col("agricultural_percentage").cast("double")) \
    .withColumn("industrial_percentage", F.col("industrial_percentage").cast("double")) \
    .withColumn("municipal_percentage", F.col("municipal_percentage").cast("double"))

# COMMAND ----------

window_spec = Window.orderBy("Year")

df_analytical = df_optimized \
    .withColumn("total_change_from_previous_year", 
                F.col("Total water withdrawal") - F.lag("Total water withdrawal").over(window_spec)) \
    .withColumn("total_percent_change_from_previous_year", 
                F.when(F.lag("Total water withdrawal").over(window_spec) != 0, 
                       (F.col("Total water withdrawal") - F.lag("Total water withdrawal").over(window_spec)) / 
                       F.lag("Total water withdrawal").over(window_spec) * 100)
                .otherwise(None))


# COMMAND ----------

df_renamed = df_analytical \
    .withColumnRenamed("Total water withdrawal", "total_water_withdrawal") \
    .withColumnRenamed("Agricultural water withdrawal", "agricultural_water_withdrawal") \
    .withColumnRenamed("Industrial water withdrawal", "industrial_water_withdrawal") \
    .withColumnRenamed("Municipal water withdrawal", "municipal_water_withdrawal")

# COMMAND ----------

df_presentation = df_renamed.select(
    "Year",
    "total_water_withdrawal",
    "agricultural_water_withdrawal",
    "industrial_water_withdrawal",
    "municipal_water_withdrawal",
    "agricultural_percentage",
    "industrial_percentage",
    "municipal_percentage",
    "total_change_from_previous_year",
    "total_percent_change_from_previous_year",
    "loaded_at"
)

# COMMAND ----------

pres_folder_path = f"{presentation_folder_path}/water_withdrawl_analysis"

# COMMAND ----------

from wtr_utils import *
gold_mount = mount_dbfs("waterprojectdl", "presentation")

# COMMAND ----------

df_presentation.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy("Year") \
    .save(f"{presentation_folder_path}/withdrawl_analysis")
