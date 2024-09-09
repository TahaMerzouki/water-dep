# Databricks notebook source
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope='water-scope')

# COMMAND ----------

dbutils.secrets.get(scope='water-scope', key='az-waterdl-acc-key')

# COMMAND ----------

# MAGIC %md
# MAGIC Now we know that the access key to the *DLG2* is safely stored in the key vault

# COMMAND ----------


