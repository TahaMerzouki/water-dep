# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read the data
improved_transformed = spark.table("wtr_processed.improved_transformed")

# Filter for Morocco
morocco_data = improved_transformed.filter(F.col("Country") == "Morocco")

# 1. Calculate year-over-year growth rates
window_spec = Window.partitionBy("Area_Type").orderBy("Year")
growth_rates = morocco_data.withColumn(
    "prev_year_access", F.lag("Access_Percentage").over(window_spec)
).withColumn(
    "yoy_growth_rate",
    (F.col("Access_Percentage") - F.col("prev_year_access")) / F.col("prev_year_access") * 100
)

# COMMAND ----------

# 2. Calculate rural-urban gap
urban_data = morocco_data.filter(F.col("Area_Type") == "urban").select(
    F.col("Year"), F.col("Access_Percentage").alias("urban_access")
)
rural_data = morocco_data.filter(F.col("Area_Type") == "rural").select(
    F.col("Year"), F.col("Access_Percentage").alias("rural_access")
)
rural_urban_gap = urban_data.join(rural_data, "Year").withColumn(
    "urban_rural_gap", F.col("urban_access") - F.col("rural_access")
)


# COMMAND ----------

# 3. Calculate overall growth rate (2000 to 2022)
overall_growth = morocco_data.groupBy("Area_Type").agg(
    (((F.max(F.when(F.col("Year") == "2022-01-01", F.col("Access_Percentage"))) -
      F.max(F.when(F.col("Year") == "2000-01-01", F.col("Access_Percentage")))) /
     F.max(F.when(F.col("Year") == "2000-01-01", F.col("Access_Percentage")))) * 100)
    .alias("overall_growth_rate")
)

# COMMAND ----------

summary_stats = morocco_data.groupBy("Area_Type").agg(
    F.mean("Access_Percentage").alias("mean_access"),
    F.expr("percentile_approx(Access_Percentage, 0.5)").alias("median_access"),
    F.min("Access_Percentage").alias("min_access"),
    F.max("Access_Percentage").alias("max_access"),
    F.variance("Access_Percentage").alias("variance_access"),
    F.stddev("Access_Percentage").alias("stddev_access")
)

# COMMAND ----------

combined_analysis = (
    growth_rates
    .join(rural_urban_gap, "Year", "left")
    .join(overall_growth, "Area_Type", "left")
    .join(summary_stats, "Area_Type", "left")
    .orderBy("Area_Type", "Year")
)

# COMMAND ----------

combined_analysis.write.mode("overwrite").parquet(f"{presentation_folder_path}/water_access_analysis")

# COMMAND ----------

combined_analysis.write.mode("overwrite").format("delta").saveAsTable("wtr_presentation.water_access_analysis")
