# Databricks notebook source
# MAGIC %run "/Workspace/Repos/Water-project/water-dep/src/includes/config"

# COMMAND ----------

from pyspark.sql.functions import col, to_date, round, regexp_extract, lit
from pyspark.sql.types import FloatType
from functools import reduce
from pyspark.sql import DataFrame

def load_and_transform_data(file_paths):
    dfs = []
    for file_path in file_paths:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        location = regexp_extract(lit(file_path), r'(\w+)_improved\.csv', 1)
        df = df.withColumn("Area_Type", location)
        dfs.append(df)
        
    combined_df = reduce(DataFrame.unionAll, dfs)
    
    # Perform transformations
    transformed_df = combined_df.select(
        col("Geographical area name").alias("Country"),
        to_date(col("Year"), "yyyy").alias("Year"),
        round(col("Value").cast(FloatType()), 2).alias("Access_Percentage"),
        col("Area_Type")
    )
    
    # Remove rows with null values
    transformed_df = transformed_df.dropna()
    
    # Sort the dataframe
    transformed_df = transformed_df.orderBy("Area_Type", "Year")
    
    return transformed_df


# COMMAND ----------


file_paths = [
    f"{raw_folder_path}/rural_improved.csv",
    f"{raw_folder_path}/urban_improved.csv",
    f"{raw_folder_path}/national_improved.csv"
]


transformed_df = load_and_transform_data(file_paths)

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_filtered = transformed_df.filter(F.col("Area_Type").isin(["urban", "rural"]))

# Pivot the data to have urban and rural as columns
df_pivoted = df_filtered.groupBy("Year").pivot("Area_Type").agg(F.first("Access_Percentage"))

# Order by Year
df_pivoted = df_pivoted.orderBy("Year")


# COMMAND ----------

df_pivoted.write.mode("overwrite").format("parquet").saveAsTable("wtr_processed.improved_pivoted")
transformed_df.write.mode("overwrite").format("parquet").saveAsTable("wtr_processed.improved_transformed")
