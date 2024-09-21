# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

water_stress_df = spark.table("wtr_processed.stress_processed_table")

# 1. Calculate year-over-year change for each stress category
window_spec = Window.orderBy("Year")
stress_categories = ["agriculture_water_stress", "industry_water_stress", "services_water_stress", "total_water_stress"]

for category in stress_categories:
    water_stress_df = water_stress_df.withColumn(
        f"{category}_yoy_change",
        F.col(category) - F.lag(category).over(window_spec)
    )

# 2. Calculate the percentage contribution of each sector to total stress
for category in stress_categories[:-1]:  # Exclude total_water_stress
    water_stress_df = water_stress_df.withColumn(
        f"{category}_percentage",
        F.col(category) / F.col("total_water_stress") * 100
    )

# 3. Identify years with significant changes (e.g., more than 5% change in total stress)
significant_changes = water_stress_df.filter(
    F.abs(F.col("total_water_stress_yoy_change")) > 3
).select("Year", "total_water_stress_yoy_change")

# 4. Calculate overall change from 2000 to 2021 for each category
overall_change = water_stress_df.filter(F.col("Year").isin(2000, 2021)).groupBy().agg(
    *[((F.max(F.when(F.col("Year") == 2021, F.col(cat))) - 
        F.max(F.when(F.col("Year") == 2000, F.col(cat)))) / 
       F.max(F.when(F.col("Year") == 2000, F.col(cat))) * 100
      ).alias(f"{cat}_overall_change") for cat in stress_categories]
)

# 5. Calculate summary statistics
summary_stats = water_stress_df.select(
    *[F.mean(cat).alias(f"{cat}_mean") for cat in stress_categories],
    *[F.stddev(cat).alias(f"{cat}_stddev") for cat in stress_categories],
    *[F.min(cat).alias(f"{cat}_min") for cat in stress_categories],
    *[F.max(cat).alias(f"{cat}_max") for cat in stress_categories]
)


# COMMAND ----------

combined_analysis = (
    water_stress_df
    .join(F.broadcast(overall_change))
    .join(F.broadcast(summary_stats))
)

# COMMAND ----------

combined_analysis.write.mode("overwrite").format("delta").save(f"{presentation_folder_path}/water_stress_analysis")

# COMMAND ----------

combined_analysis.write.format("delta").mode("overwrite").saveAsTable("wtr_presentation.water_stress_analysis")
