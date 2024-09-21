# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, FloatType

# COMMAND ----------

water_withdrawl = spark.read.csv(f"{raw_folder_path}/water_withdrawl_raw.csv", header=True)
print(f"schema: {water_withdrawl.printSchema()}")
print(f"original count: {water_withdrawl.count()}")

# COMMAND ----------

import sys
folder_path = sys.path.append("/Workspace/Repos/Water-project/water-dep/src/includes")

# COMMAND ----------

from process import remove_specific_rows
df = remove_specific_rows(water_withdrawl, 93, 128)
print(df.count())

# COMMAND ----------


# 2. Clean and preprocess
df_renamed = df.withColumn("Year", col("Year").cast("integer")) \
                            .withColumn("Value", col("Value").cast("float"))

# 3. Standardize column names
df_renamed = df_renamed.select(
    col("Year"),
    col("Indicator name").alias("indicator_name"),
    col("Value"),
    col("Units").alias("units")
)

# COMMAND ----------

df_filtered = df_renamed.filter(col("Value").isNotNull())

# COMMAND ----------

df_pivoted = df_filtered.groupBy("Year") \
    .pivot("indicator_name") \
    .agg({"Value": "first"}) \
    .orderBy("Year")

# COMMAND ----------

display(df_pivoted)

# COMMAND ----------

df_calculated = df_pivoted.withColumn(
    "agricultural_percentage",
    col("Agricultural water withdrawal") / col("Total water withdrawal") * 100
).withColumn(
    "industrial_percentage",
    col("Industrial water withdrawal") / col("Total water withdrawal") * 100
).withColumn(
    "municipal_percentage",
    col("Municipal water withdrawal") / col("Total water withdrawal") * 100
)

# COMMAND ----------

df_calculated.write.mode("overwrite").partitionBy("Year").format("parquet").saveAsTable("wtr_processed.withdrawl_perc")
df_pivoted.write.mode("overwrite").partitionBy("Year").format("parquet").saveAsTable("wtr_processed.withdrawl_pivoted")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wtr_processed.withdrawl_pivoted WHERE Year BETWEEN 2000 AND 2022 ORDER BY Year;