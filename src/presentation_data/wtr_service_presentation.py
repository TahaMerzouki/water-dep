# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

water_service_df = spark.read.parquet(f"{processed_folder_path}/overall_table", header=True)

# Rename columns to remove spaces and commas for easier handling
water_service_df = water_service_df.withColumnRenamed("Indicator name", "indicator_name") \
                                   .withColumnRenamed("Geographical area name", "country") \
                                   .withColumnRenamed("Value", "service_percentage")

# Filter for Morocco (though it seems all data is for Morocco already)
morocco_water_df = water_service_df.filter(F.col("country") == "Morocco")

# Convert Year to DateType for easier time-based operations
morocco_water_df = morocco_water_df.withColumn("Year", F.to_date(F.col("Year").cast("string"), "yyyy"))

# Calculate year-over-year change
window_spec = Window.orderBy("Year")
morocco_water_df = morocco_water_df.withColumn(
    "yoy_change",
    F.col("service_percentage") - F.lag("service_percentage").over(window_spec)
)

# Calculate overall change from 2000 to 2022
overall_change = morocco_water_df.filter(F.col("Year").isin("2000-01-01", "2022-01-01")).groupBy().agg(
    ((F.max(F.when(F.col("Year") == "2022-01-01", F.col("service_percentage"))) -
      F.max(F.when(F.col("Year") == "2000-01-01", F.col("service_percentage")))) /
     F.max(F.when(F.col("Year") == "2000-01-01", F.col("service_percentage"))) * 100
    ).alias("overall_percentage_change")
)

# Calculate average annual growth rate
num_years = 22  # 2000 to 2022
morocco_water_df = morocco_water_df.withColumn(
    "avg_annual_growth_rate",
    (F.pow(F.col("service_percentage") / F.first("service_percentage").over(window_spec), 1/num_years) - 1) * 100
)

# Identify years with significant changes (e.g., more than 2% change)
significant_changes = morocco_water_df.filter(F.abs(F.col("yoy_change")) > 2)

# Calculate summary statistics
summary_stats = morocco_water_df.select(
    F.mean("service_percentage").alias("mean_service_percentage"),
    F.stddev("service_percentage").alias("stddev_service_percentage"),
    F.min("service_percentage").alias("min_service_percentage"),
    F.max("service_percentage").alias("max_service_percentage")
)

# COMMAND ----------

combined_analysis = morocco_water_df.join(F.broadcast(overall_change))

# COMMAND ----------

combined_analysis.write.format("delta").mode("overwrite").save(f"{presentation_folder_path}/water_service_analysis")

combined_analysis.write.format("delta").mode("overwrite").saveAsTable("wtr_presentation.water_service_analysis")
