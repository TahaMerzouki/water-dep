# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='water-scope')

# COMMAND ----------

dbutils.secrets.get(scope='water-scope', key='waterapp-client-id')

# COMMAND ----------

# MAGIC %md
# MAGIC Now we know that the access key to the *DLG2* is safely stored in the key vault

# COMMAND ----------

from wtr_utils import mount_dbfs
mount = mount_dbfs("waterprojectdl", "raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying one of the tables
# MAGIC

# COMMAND ----------

display(spark.read.csv("/mnt/waterprojectdl/raw/population_using_water_safely_overall.csv", header=True))

# COMMAND ----------

display(dbutils.fs.mounts())
