# Databricks notebook source
# MAGIC %md
# MAGIC ## Mounting the Storage Account
# MAGIC 
# MAGIC Access with a Application databricks-service-app with the Role of Storage Blob Data Contributor 
# MAGIC 
# MAGIC ### Secret Scopes
# MAGIC 
# MAGIC Secret scopes help store the credentials securely and reference them in notebooks and jobs when required
# MAGIC 
# MAGIC * Databricks Backed Secret Scope
# MAGIC * Azure Backed Key Secret Scope
# MAGIC 
# MAGIC I will be using Azure Key-Vault, connecting it to Databricks Secret Scope and then get secrets using the dbutils.secrets.get function.

# COMMAND ----------

# unmount dls
storage_account_name = 'frmla1dl'
containers = ['raw', 'processed']

mounts = dbutils.fs.mounts()

for mount in mounts:
    for container in containers:
        if container in mount:
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container}/")
            dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container}/")

# COMMAND ----------

# define scope and list secrets created from #secrets/createScope on the databricks homepage.

secret_scope = 'frmla1scope'
secret_list = dbutils.secrets.list(secret_scope)

# print secret lists stored in Scope

for i in secret_list:
    print(i)
    
# Set ids and secrets with dbutils.secrets.get()    

client_id = dbutils.secrets.get(secret_scope, 'databricks-app-client-id')
client_secret = dbutils.secrets.get(secret_scope, 'databricks-app-client-secret')
tenant_id = dbutils.secrets.get(secret_scope, 'databricks-app-tenant-id')

# COMMAND ----------

#Set Configs

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_container(storage_account_name, container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/P{storage_account_name}/{container_name}",
        extra_configs = configs)
    return

# COMMAND ----------

# Mount Containers
mount_container("frmula1dl", "raw")
mount_container("frmula1dl", "processed")

# List all the mounts in the workspace
dbutils.fs.mounts()
