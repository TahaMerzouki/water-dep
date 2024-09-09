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

client_id = dbutils.secrets.get(scope='water-scope', key='waterapp-client-id')
tenant_id = dbutils.secrets.get(scope='water-scope', key='waterapp-tenant-id')
client_secret = dbutils.secrets.get(scope='water-scope', key='waterapp-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://raw@waterprojectdl.dfs.core.windows.net/",
  mount_point = "/mnt/waterprojectdl/raw",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/waterprojectdl/raw"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Displaying one of the tables
# MAGIC

# COMMAND ----------

display(spark.read.csv("/mnt/waterprojectdl/raw/population_using_water_safely_overall.csv", header=True))

# COMMAND ----------

display(dbutils.fs.mounts())
