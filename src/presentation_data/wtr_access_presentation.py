# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wtr_processed.improved_pivoted;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wtr_processed.improved_transformed;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wtr_processed.overall_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Calculate year-over-year growth rates
# MAGIC WITH growth_rates AS (
# MAGIC   SELECT 
# MAGIC     Year,
# MAGIC     Area_Type,
# MAGIC     Access_Percentage,
# MAGIC     (Access_Percentage - LAG(Access_Percentage) OVER (PARTITION BY Area_Type ORDER BY Year)) 
# MAGIC       / LAG(Access_Percentage) OVER (PARTITION BY Area_Type ORDER BY Year) * 100 AS yoy_growth_rate
# MAGIC   FROM wtr_processed.improved_transformed
# MAGIC   WHERE Country = 'Morocco'
# MAGIC ),
# MAGIC
# MAGIC -- 2. Calculate rural-urban gap
# MAGIC rural_urban_gap AS (
# MAGIC   SELECT 
# MAGIC     u.Year,
# MAGIC     u.Access_Percentage - r.Access_Percentage AS urban_rural_gap
# MAGIC   FROM wtr_processed.improved_transformed u
# MAGIC   JOIN wtr_processed.improved_transformed r ON u.Year = r.Year
# MAGIC   WHERE u.Area_Type = 'urban' AND r.Area_Type = 'rural' AND u.Country = 'Morocco' AND r.Country = 'Morocco'
# MAGIC ),
# MAGIC
# MAGIC -- 3. Calculate overall growth rate (2000 to 2022)
# MAGIC overall_growth AS (
# MAGIC   SELECT
# MAGIC     Area_Type,
# MAGIC     (MAX(CASE WHEN Year = '2022-01-01' THEN Access_Percentage END) - 
# MAGIC      MAX(CASE WHEN Year = '2000-01-01' THEN Access_Percentage END)) / 
# MAGIC      MAX(CASE WHEN Year = '2000-01-01' THEN Access_Percentage END) * 100 AS overall_growth_rate
# MAGIC   FROM wtr_processed.improved_transformed
# MAGIC   WHERE Country = 'Morocco'
# MAGIC   GROUP BY Area_Type
# MAGIC ),
# MAGIC
# MAGIC -- 4. Calculate summary statistics
# MAGIC summary_stats AS (
# MAGIC   SELECT
# MAGIC     Area_Type,
# MAGIC     AVG(Access_Percentage) AS mean_access,
# MAGIC     PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY Access_Percentage) AS median_access,
# MAGIC     MIN(Access_Percentage) AS min_access,
# MAGIC     MAX(Access_Percentage) AS max_access,
# MAGIC     VARIANCE(Access_Percentage) AS variance_access,
# MAGIC     STDDEV(Access_Percentage) AS stddev_access
# MAGIC   FROM wtr_processed.improved_transformed
# MAGIC   WHERE Country = 'Morocco'
# MAGIC   GROUP BY Area_Type
# MAGIC )
# MAGIC
# MAGIC -- Combine all the analyses
# MAGIC SELECT 
# MAGIC   g.Year,
# MAGIC   g.Area_Type,
# MAGIC   g.Access_Percentage,
# MAGIC   g.yoy_growth_rate,
# MAGIC   rug.urban_rural_gap,
# MAGIC   og.overall_growth_rate,
# MAGIC   ss.mean_access,
# MAGIC   ss.median_access,
# MAGIC   ss.min_access,
# MAGIC   ss.max_access,
# MAGIC   ss.variance_access,
# MAGIC   ss.stddev_access
# MAGIC FROM growth_rates g
# MAGIC LEFT JOIN rural_urban_gap rug ON g.Year = rug.Year
# MAGIC LEFT JOIN overall_growth og ON g.Area_Type = og.Area_Type
# MAGIC LEFT JOIN summary_stats ss ON g.Area_Type = ss.Area_Type
# MAGIC ORDER BY g.Area_Type, g.Year;
# MAGIC

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

display(combined_analysis)

# COMMAND ----------

combined_analysis.write.mode("overwrite").parquet(f"{presentation_folder_path}/water_access_analysis")

# COMMAND ----------

combined_analysis.write.mode("overwrite").format("delta").saveAsTable("wtr_presentation.water_access_analysis")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wtr_presentation.water_access_analysis;
