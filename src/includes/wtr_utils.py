from pyspark.dbutils import DBUtils

def mount_dbfs(storage_account_name, container_name):
    dbutils = DBUtils()

    # Generate the mount point
    mount_point = f"/mnt/{storage_account_name}/{container_name}"
    
    print(f"Attempting to mount {container_name} to {mount_point}")

    # Water project secrets
    client_id = dbutils.secrets.get(scope='water-scope', key='waterapp-client-id')
    tenant_id = dbutils.secrets.get(scope='water-scope', key='waterapp-tenant-id')
    client_secret = dbutils.secrets.get(scope='water-scope', key='waterapp-client-secret')

    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    }

    # Check if the mount point or container is already mounted
    mounts = dbutils.fs.mounts()
    for mount in mounts:
        if mount.mountPoint == mount_point:
            print(f"Mount point {mount_point} is already in use. Returning existing mount point.")
            return mount_point
        elif f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/" in mount.source:
            print(f"Container {container_name} is already mounted at {mount.mountPoint}")
            return mount.mountPoint

    # If not mounted, proceed with mounting
    print(f"Mounting {container_name} to {mount_point}")
    try:
        dbutils.fs.mount(
            source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
            mount_point = mount_point,
            extra_configs = configs 
        )
        print(f"Successfully mounted {container_name} to {mount_point}")
    except Exception as e:
        print(f"Error while mounting: {str(e)}")
        return None

    return mount_point

def list_adls_contents(mount_point):
    """
    Lists the contents of a mounted ADLS container.
    
    :param mount_point: The DBFS path where the container is mounted
    """
    dbutils = DBUtils()
    contents = dbutils.fs.ls(mount_point)
    for item in contents:
        print(f"{item.name} ({'directory' if item.isDir() else 'file'})")