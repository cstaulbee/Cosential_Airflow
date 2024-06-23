from azure.storage.blob import BlobServiceClient
import json
import os
from azure.identity import DefaultAzureCredential
from plugins.azure_utils import get_secret
from azure.keyvault.secrets import SecretClient
from sqlalchemy import create_engine, Column, Integer, String, MetaData, Table, Float, DateTime, Text, inspect, Boolean
from sqlalchemy.orm import sessionmaker
import urllib

adls_connection_string_secret_name = os.getenv('ADLS_CONNECTION_STRING_SECRET')
keyvault_url = os.environ.get("KEYVAULT_URL")
sql_password_secret_name = os.getenv('SQL_PW_SECRET')

#Defines the connection string to the STORAGE ACCOUNT in Azure Data Lake Storage (ADLS)
adls_connection_string = get_secret(adls_connection_string_secret_name)

# Defines the elements of the connection string to the SQL SERVER in Azure SQL Database
server = os.environ.get("SQL_SERVER")
database = os.environ.get("DATABASE")
sql_username = os.environ.get("SQL_USER")
sql_password = get_secret(sql_password_secret_name)
driver = os.environ.get("DRIVER")

def get_secret(secret_name):
    """
    Retrieves a secret value from Azure Key Vault.

    Args:
        secret_name (str): The name of the secret to retrieve.

    Returns:
        str: The value of the secret.

    Raises:
        azure.core.exceptions.ResourceNotFoundError: If the secret does not exist in Azure Key Vault.
        azure.core.exceptions.ClientAuthenticationError: If the authentication fails.
        azure.core.exceptions.ServiceRequestError: If there is an error making the request to Azure Key Vault.
    """
    # Create an instance of the DefaultAzureCredential class. Will look for environment variables first, then managed identity, then interactive login.
    credential = DefaultAzureCredential()

    # Create an instance of the SecretClient class
    client = SecretClient(vault_url=keyvault_url, credential=credential)

    # Retrieve the secret value from Azure Key Vault
    secret = client.get_secret(secret_name).value
    return secret



def write_data_azure_storage(data_export, container_name, entity_name, blob_name):
    """
    Uploads JSON data to Azure Storage Blob.

    Args:
        data_export (dict): The JSON data to upload.
        container_name (str): The name of the Azure Storage container.
        entity_name (str): The name of the entity.
        connection_string (str): The connection string for Azure Storage.

    Returns:
        str: The URL of the uploaded blob.

    Raises:
        Exception: If an error occurs during the upload process.
    """
    try:
        # Debugging: Print the JSON data to be uploaded
        entity = entity_name["Entity"]
        print(f"Uploading JSON data for entity: {entity}")
        
        
        blob_service_client = BlobServiceClient.from_connection_string(adls_connection_string)
        blob_path = os.path.join(entity, blob_name)
        print(f"Blob path: {blob_path}")
        
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_path)
        
        blob_client.upload_blob(data_export, overwrite=True)
        
        print(f"Upload Successful: JSON data for {entity} to {container_name}/{blob_path}")
        return f'https://{blob_service_client.account_name}.blob.core.windows.net/{container_name}/{blob_path}'
    except Exception as e:
        print(f"Error: {e}")
        return False



def read_from_azure_storage(container_name, blob_name):
    """
    Reads data from Azure Storage Blob.

    Args:
        container_name (str): The name of the Azure Storage container.
        blob_name (str): The name of the blob.
        connection_string (str): The connection string for Azure Storage.

    Returns:
        dict: The JSON data read from the blob.

    Raises:
        FileNotFoundError: If the blob is not found.
        Exception: If an error occurs during the read process.
    """
    try:
        blob_service_client = BlobServiceClient.from_connection_string(adls_connection_string)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        download_stream = blob_client.download_blob().readall()
        json_data = json.loads(download_stream)
        print(f"Download Successful: {container_name}/{blob_name}")
        return json_data
    except FileNotFoundError:
        print("The file was not found")
        return False
    except Exception as e:
        print(f"Error: {e}")
        return False
    

def create_azure_engine():
    """
    Create and return an Azure SQL database engine.

    Args:
        server (str): The server name or IP address.
        database (str): The name of the database.
        username (str): The username for authentication.
        password (str): The password for authentication.

    Returns:
        sqlalchemy.engine.Engine: The created Azure SQL database engine.
    """
    params = urllib.parse.quote_plus(
        f'Driver={driver};Server=tcp:{server},1433;Database={database};Uid={sql_username};Pwd={sql_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    )
    sql_conn_str = 'mssql+pyodbc:///?odbc_connect={}'.format(params)
    engine_azure = create_engine(sql_conn_str, echo=True)

    return engine_azure



def write_data_azure_sql(transformed_data, table_name, list_columns):
    """
    Loads transformed data into an Azure SQL database table.

    Args:
        transformed_data (list): The transformed data to be loaded into the table.
        server (str): The name of the Azure SQL server.
        database (str): The name of the Azure SQL database.
        username (str): The username for connecting to the Azure SQL server.
        password (str): The password for connecting to the Azure SQL server.
        table_schema (list): The schema of the table in the form of a list of dictionaries.

    Returns:
        None
    """
    engine_azure = create_azure_engine() #old version create_engine(f'mssql+pyodbc://{sql_username}:{sql_password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server')
    metadata = MetaData()

    # Define a dictionary to map the string types in your schema to SQLAlchemy types
    type_mapping = {
        'Integer': Integer,
        'String': String,
        'Float': Float,
        'DateTime': DateTime,
        'Text': Text,
        'Boolean': Boolean,
        'Decimal': Float
    }

    # Create a list of Column objects based on table_schema
    columns = []
    for column in list_columns:
        column_type = type_mapping[column['type']]
        if column_type is String:
            columns.append(Column(column['name'], column_type(column['length'])))
        else:
            columns.append(Column(column['name'], column_type))

    # Create the table
    table = Table(table_name, metadata, *columns)

    # Create the table if it doesn't exist
    inspector = inspect(engine_azure)
    if not inspector.has_table(table_name):
        table.create(engine_azure)

    # Create session
    Session = sessionmaker(bind=engine_azure)
    session = Session()
    
    # Insert data
    with engine_azure.connect() as connection:
        for row in transformed_data:
            insert_query = table.insert().values(dict(zip(table.columns.keys(), row)))
            connection.execute(insert_query)
    
    session.commit()
    session.close()
