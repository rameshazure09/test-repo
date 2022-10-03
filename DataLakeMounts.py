# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ###Mounts to Data Lake Azure Storage Account for Workspace
# MAGIC 
# MAGIC #### Configuration
# MAGIC 
# MAGIC ##### Key Vault
# MAGIC * DatalakeAccountName => Storage Account Name (example: datalake)
# MAGIC * DatalakeAccountKey => Storage Account Access Key (example: xyzk9)
# MAGIC 
# MAGIC #### Containers
# MAGIC * deltalake
# MAGIC * enhanced
# MAGIC * normalized
# MAGIC * rawstream
# MAGIC 
# MAGIC #### Mounts
# MAGIC * /mnt/datalake/deltalake => deltalake container
# MAGIC * /mnt/datalake/enhanced => enhanced container
# MAGIC * /mnt/datalake/normalized => normalized container
# MAGIC * /mnt/datalake/rawstream => rawstream container

# COMMAND ----------

# dbutils.fs.mounts()

# COMMAND ----------

def Mount(storageContainer, storageAccountName = "DatalakeAccountName", storageAccountKey = "DatalakeAccountKey", deltaLakePath = "datalake"):
  datalakeAccountName = dbutils.secrets.get(scope = "scope_name", key = storageAccountName)
  datalakeAccountKey = dbutils.secrets.get(scope = "scope_name", key = storageAccountKey)

  configs = {
            f"fs.azure.account.key.{datalakeAccountName}.blob.core.windows.net": datalakeAccountKey 
            }  
  
  mountSource = f"wasbs://{storageContainer}@{datalakeAccountName}.blob.core.windows.net"
  mountPoint = f"/mnt/{deltaLakePath}/{storageContainer}"

  #print(mountSource)
  #print(mountPoint)
  
  if any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
    dbutils.fs.unmount(mountPoint)

  dbutils.fs.mount(source = mountSource, mount_point = mountPoint, extra_configs = configs)

# COMMAND ----------

Mount("rawstream")
Mount("normalized")
Mount("enhanced")
Mount("deltalake")
Mount("batch")

# COMMAND ----------

Mount("rawstream", "DataLakeForscope_nameSIT-AccountName", "DataLakeForscope_nameSIT-AccountKey", "datalakeforscope_namesit")
Mount("normalized", "DataLakeForscope_nameSIT-AccountName", "DataLakeForscope_nameSIT-AccountKey", "datalakeforscope_namesit")
Mount("enhanced", "DataLakeForscope_nameSIT-AccountName", "DataLakeForscope_nameSIT-AccountKey", "datalakeforscope_namesit")
Mount("deltalake", "DataLakeForscope_nameSIT-AccountName", "DataLakeForscope_nameSIT-AccountKey", "datalakeforscope_namesit")

# COMMAND ----------

Mount("rawstream", "DataLakeForscope_nameUAT-AccountName", "DataLakeForscope_nameUAT-AccountKey", "datalakeforscope_nameuat")
# COMMAND ----------

Mount("rawstream", "DataLakeTESTprd-AccountName", "DataLakeTESTprd-AccountKey", "datalakeTESTprd")
