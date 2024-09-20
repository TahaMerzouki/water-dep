# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest population using water safelly overall

# COMMAND ----------

# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

overall = spark.read.csv(f"{raw_folder_path}/population_using_water_safely_overall.csv", header=True)

# COMMAND ----------

import sys
sys.path.append("/Workspace/Repos/Water-project/water-dep/src/includes")

# COMMAND ----------

from process import remove_specific_rows
overall_num_cleaned = remove_specific_rows(overall, 24, 59)

# COMMAND ----------

from pyspark.sql.functions import col

columns = overall_num_cleaned.columns

null_columns = [column for column in columns if overall_num_cleaned.filter(col(column).isNotNull()).count() == 0]

null_columns

# COMMAND ----------

overall_cleaned_col = overall_num_cleaned.drop(*null_columns, "SDG indicator", "SDG 6 Data portal level")
display(overall_cleaned_col)

# COMMAND ----------

overall_cleaned_col = overall_cleaned_col.withColumn("Value", col("Value").cast("double")).withColumn("Year", col("Year").cast("integer"))

# COMMAND ----------

overall_cleaned_col.printSchema()

# COMMAND ----------

display(overall_cleaned_col.select("Year","Value").orderBy("Year"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write the dataframe in a parquet file to processed folder
# MAGIC

# COMMAND ----------

from wtr_utils import mount_dbfs
processed_mount = mount_dbfs("waterprojectdl", "processed")

# COMMAND ----------

overall_cleaned_col.write.mode("overwrite").format("parquet").saveAsTable("wtr_processed.overall_table")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM wtr_processed.overall_table;

# COMMAND ----------

df = spark.read.parquet(f"{processed_folder_path}/overall")
display(df)
