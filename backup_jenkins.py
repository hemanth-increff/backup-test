import mysql.connector
import pandas as pd
from datetime import datetime
from azure.cli.core import get_default_cli

def get_authed_cli(config):
    client_id = config["client_id"]
    secret = config["secret"]
    tenant = config["tenant"]
    azcli = get_default_cli()
    azcli.invoke(
        [
            "login",
            "--service-principal",
            "-u",
            client_id,
            "-p",
            secret,
            "--tenant",
            tenant,
        ]
    )
    return azcli

def copy_container(source, destination, python_config):
    print(f"Copying from {source} to {destination}")
    try:
        azcli = get_authed_cli(python_config)
    except Exception as e:
        print("Error logging in to azcli")
    try:
        azcli.invoke(
            [
                "storage",
                "copy",
                "-s",
                source,
                "-d",
                destination,
                "--recursive",
            ]
        )
    except Exception as e:
        print("Error copying files")
    print(f"Copy completed for {source}")
    
db_user = "nextscm-app-user"
db_pass = "nextscm@fashion"
db_host = "mysql-manager-ms-enterprise-dev-increff.mysql.database.azure.com"
db_schema = "master"

cnx = mysql.connector.connect(
    user=db_user, password=db_pass, host=db_host, database=db_schema
)
cursor= cnx.cursor()
source_storage_account_name = "stincrfmsedcelio"
destination_storage_account_name = "stincrfmsedcelio"
L1_destination_storage_account_name = "stincreffmsenterprisdev"
L1_destination_container_name = "backup-test"
print("Getting values from DB...")
try:
    cursor.execute(f"SELECT distinct(storage_container_name), workspace_name FROM master.client c inner join master.project p on p.client_id = c.id where storage_account_name = '{source_storage_account_name}' ;")
    rows = cursor.fetchall()
except Exception as e:
    print("Failed to fetch from DB!!!")
print("Fetched containers from DB...")

details = pd.DataFrame(rows, columns=[column[0] for column in cursor.description])
date = datetime.today().strftime('%Y-%m-%d')
python_config = {
    "client_id":"b56b3d7a-0438-49ee-9532-da90bacf5b0a",
    "secret":"ef14e291-3b4d-42b7-bc0d-acb7cad4403c",
    "tenant":"69383bf3-9a5d-4dbd-b16d-cfd47d4c5714"
}
workspace = "synw-celio-ms-enterprise-dev-increff"
for index, row in details.iterrows():
    workspace = row['workspace_name']
    if row['storage_container_name'] == row['workspace_name']:
        continue
    source = f"https://{source_storage_account_name}.blob.core.windows.net/{row['storage_container_name']}/input"
    destination = f"https://{destination_storage_account_name}.blob.core.windows.net/{row['workspace_name']}/backup/{date}/{row['storage_container_name']}/"
    print(f"Copying {row['storage_container_name']}")
    try:
        copy_container(source, destination, python_config)
    except Exception as e:
        print("Failed to copy")

    print(f"Completed copying {row['storage_container_name']}")


print("Performing L1 Backup")
L1_source = f"https://{destination_storage_account_name}.blob.core.windows.net/{workspace}/backup/{date}/"
L1_destination = f"https://{L1_destination_storage_account_name}.blob.core.windows.net/{L1_destination_container_name}/{source_storage_account_name}/"
try:
    copy_container(L1_source, L1_destination, python_config)
except Exception as e:
    print("Failed to copy")
