# Databricks notebook source
# MAGIC %run "../includes/config"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, FloatType

stress_withdrawl = spark.read.csv(f"{raw_folder_path}/stress_freshwater_withdrawl_proportion.csv", header=True)

# COMMAND ----------

# Remove rows where all columns are null
stress_withdrawl = stress_withdrawl.dropna(how='all')

# Display the column names
print("Available columns:")
print(stress_withdrawl.columns)

# Select relevant columns
columns_to_select = ['Year', 'Value']
sdg_column = [col for col in stress_withdrawl.columns if 'SDG' in col and 'level' in col.lower()]

if sdg_column:
    columns_to_select.append(F.col(sdg_column[0]).alias('SDG_level'))

stress_withdrawl = stress_withdrawl.select(*columns_to_select)


# COMMAND ----------

# Convert Year to integer and Value to float
stress_withdrawl = stress_withdrawl.withColumn('Year', F.col('Year').cast(IntegerType())) \
       .withColumn('Value', F.col('Value').cast(FloatType()))

# COMMAND ----------

def extract_sector(text):
    if 'Agriculture, forestry and fishing' in text:
        return 'Agriculture'
    elif 'Industry' in text:
        return 'Industry'
    elif 'Services' in text:
        return 'Services'
    else:
        return 'Unknown'

extract_sector_udf = F.udf(extract_sector)

stress_withdrawl = stress_withdrawl.withColumn('Sector', extract_sector_udf(F.col('SDG_level')))
stress_withdrawl.groupBy('Sector').count().show()

# COMMAND ----------

stress_pivoted = stress_withdrawl.groupBy('Year').pivot('Sector').agg(F.first('Value'))

# COMMAND ----------

df_final = stress_pivoted.withColumn('total_water_stress', 
                                 F.coalesce(F.col('Agriculture'), F.lit(0)) + 
                                 F.coalesce(F.col('Industry'), F.lit(0)) + 
                                 F.coalesce(F.col('Services'), F.lit(0)))

# Rename columns
df_final = df_final.withColumnRenamed('Agriculture', 'agriculture_water_stress') \
                   .withColumnRenamed('Industry', 'industry_water_stress') \
                   .withColumnRenamed('Services', 'services_water_stress')

# COMMAND ----------

df_final = df_final.orderBy('Year')

# COMMAND ----------

df_final.write.mode("overwrite").format("parquet").saveAsTable("wtr_processed.stress_processed_table")
stress_pivoted.write.mode("overwrite").format("parquet").saveAsTable("wtr_processed.stress_pivoted")

# COMMAND ----------

stress = spark.read.parquet("/mnt/waterprojectdl/processed/water_stress_pivoted")
