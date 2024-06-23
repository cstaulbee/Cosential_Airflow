# Standard library imports
import json
import logging
import os
from datetime import datetime, timedelta

# Third-party imports
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from azure.storage.blob import BlobServiceClient

# Local application imports
from plugins.api_utils import fetch_changed_ids, pull_entity_objects, pull_entity_arrays, update_latest_version
from plugins.azure_utils import read_from_azure_storage, get_secret, write_data_azure_storage, write_data_azure_sql


blob_container = os.environ['BLOB_CONTAINER']

# Read metadata from Azure Storage
entities = read_from_azure_storage(blob_container, 'metadata/cosential_entities.json')
schema = read_from_azure_storage(blob_container, 'metadata/Cosential_Table_Schemas.json')

# Set up environment variables and paths
airflow_home = os.environ['AIRFLOW_HOME']
source_name = os.environ['SOURCE_NAME']

# Define paths to the DBT project and virtual environment
PATH_TO_DBT_PROJECT = f'{airflow_home}/dbt_project'
PATH_TO_DBT_VENV = f'{airflow_home}/dbt_venv/bin/activate'
PATH_TO_DBT_VARS = f'{airflow_home}/dbt_project/dbt.env'
ENTRYPOINT_CMD = f"source {PATH_TO_DBT_VENV} && source {PATH_TO_DBT_VARS}"

# Get current date and format it
now = datetime.now()
year = now.strftime('%Y')
month = now.strftime('%m')
day = now.strftime('%d')

# Define blob path
blob_path = os.path.join(f"year={year}", f"month={month}", f"day={day}")

# Temporary file name
hour = 'temp'


def count_files_with_prefix(blob_path, connection_string):

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(blob_container)

    prefix = f'{blob_path}/run='
    blob_list = container_client.list_blobs(name_starts_with=prefix)

    count = 0
    for blob in blob_list:
        count += 1

    return count


def create_dynamic_tasks():
    """
    Creates dynamic tasks for processing entities and their tables.

    This function iterates over a list of entities and creates two PythonOperator tasks for each entity:
    - process_entity_task: Executes the `process_entity` function with the given entity and blob_path as arguments.
    - process_entity_tables_task: Executes the `process_entity_tables` function with the given schema and entity_name as arguments.

    The `process_entity_task` is set as the upstream task for the `process_entity_tables_task`.

    Parameters:
        None

    Returns:
        None
    """
    for entity in entities:
        entity_name = entity['Entity']

        process_entity_task = PythonOperator(
            task_id=f'process_entity_{entity["Entity"]}',
            python_callable=process_entity,
            op_kwargs={'entity': entity, 'blob_path': blob_path},
        )

        process_entity_tables_task = PythonOperator(
            task_id=f'process_entity_tables_{entity_name}',
            python_callable=process_entity_tables,
            op_kwargs={'schema': schema, 'entity_name': entity_name},
        )

        process_entity_task >> process_entity_tables_task



def get_secret_from_keyvault(secret_name, xcom_key, **kwargs):
    """
    Retrieves a secret value from Key Vault and pushes it to XCom.

    Args:
        secret_name (str): The name of the secret in Key Vault.
        xcom_key (str): The key to use when pushing the secret value to XCom.
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    secret_value = get_secret(secret_name) # this function is defined in azure_utils.py
    kwargs['ti'].xcom_push(key=xcom_key, value=secret_value)


def get_metadata(source, blob_path, xcom_key, **kwargs):
    """
    Retrieves metadata from Azure storage and pushes it to XCom.

    Args:
        source (str): The source of the Azure storage.
        blob_path (str): The path to the blob in Azure storage.
        xcom_key (str): The key to use when pushing the metadata to XCom.
        **kwargs: Additional keyword arguments.

    Returns:
        None
    """
    output = read_from_azure_storage(source, blob_path) # this function is defined in azure_utils.py
    kwargs['ti'].xcom_push(key=xcom_key, value=output)


def fetch_and_write_latest_versions(**kwargs):
    """
    Fetches the latest versions of entities, writes them to Azure Storage, and logs the value of latest_versions.

    Parameters:
    - kwargs: A dictionary of keyword arguments.

    Returns:
    None
    """
    # Fetch latest versions
    ti = kwargs['ti']

    entities = ti.xcom_pull(task_ids='get_entities', key='entities')

    latest_versions = update_latest_version(entities) # this function is defined in plugins/api_utils.py. It fetches the latest versions of entities.
    latest_versions_json = json.dumps(latest_versions)

    # Write latest versions to Azure Storage
    write_data_azure_storage(latest_versions_json, blob_container, {'Entity':'metadata'}, 'latest_run_versions.json') # this function is defined in plugins/azure_utils.py. It writes data to ADLS in the .

    # Log the value of latest_versions
    logging.info(f"Latest Versions: {latest_versions}")

def process_entity(entity, **kwargs):
    ti = kwargs['ti']

    versions = ti.xcom_pull(task_ids='get_versions', key='versions')
    entity_arrays = ti.xcom_pull(task_ids='get_arrays', key='entity_arrays')

    blob_path = kwargs['blob_path']

    # Fetch changed IDs based on versions
    changed_ids = fetch_changed_ids(entity, versions)
    # Pull entity objects using changed IDs
    entity_results = pull_entity_objects(entity, changed_ids)

    for entity_array in entity_arrays:
        if entity_array['Entity'] == entity['Entity']:
            for array in entity_array['Arrays']:
                entity_array_values = pull_entity_arrays(entity, array, changed_ids)
                entity_array_json = json.dumps(entity_array_values, ensure_ascii=False, indent=4)
                write_data_azure_storage(entity_array_json, blob_container, entity, os.path.join(blob_path, f'{array}.json'))

    # Serialize and store the processed data
    processed_data_json = json.dumps(entity_results, ensure_ascii=False, indent=4)
    write_data_azure_storage(processed_data_json, blob_container, entity, os.path.join(blob_path, f"run={hour}.json"))

    return entity_results

def process_entity_tables(schema, entity_name):

    for entity_dict in schema['Entities']:
        if entity_dict['Entity'] == entity_name:
            for table_dict in entity_dict['tables']:
                table_name = table_dict['table_name']
                file_name = table_dict['file_name']
                columns = table_dict['columns']

                if file_name == 'default':
                    blob_name = os.path.join(entity_name, f"year={year}", f"month={month}", f"day={day}", f"run={hour}.json")
                    print(blob_name)
                else:
                    blob_name = os.path.join(entity_name, f"year={year}", f"month={month}", f"day={day}", file_name)
                    print(blob_name)

                opp = read_from_azure_storage(blob_container, blob_name)

                if not opp:
                    raise ValueError(f"Failed to read data from Azure storage for blob {blob_name}")

                transformed_data = [
                    tuple(item.get(column['name']) for column in columns) for item in opp
                ]

                write_data_azure_sql(transformed_data, table_name, columns)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
with DAG(
    'entity_processing_dag',
    default_args=default_args,
    description='A simple DAG to process entities',
    schedule_interval=None,
    start_date=days_ago(2)
) as dag:

    get_entities_task = PythonOperator(
        task_id='get_entities',
        python_callable=get_metadata,
        provide_context=True,
        op_kwargs={
            'source': blob_container,
            'blob_path': 'metadata/cosential_entities.json',
            'xcom_key': 'entities'
        }
    )

    get_versions_task = PythonOperator(
        task_id='get_versions',
        python_callable=get_metadata,
        provide_context=True,
        op_kwargs={
            'source': blob_container,
            'blob_path': 'metadata/latest_run_versions.json',
            'xcom_key': 'versions'
        }
    )

    get_entity_arrays_task = PythonOperator(
        task_id='get_arrays',
        python_callable=get_metadata,
        provide_context=True,
        op_kwargs={
            'source': blob_container,
            'blob_path': 'metadata/cosential_arrays.json',
            'xcom_key': 'entity_arrays'
        }
    )

    get_schema_task = PythonOperator(
        task_id='get_schema',
        python_callable=get_metadata,
        provide_context=True,
        op_kwargs={
            'source': blob_container,
            'blob_path': 'metadata/Cosential_Table_Schemas.json',
            'xcom_key': 'schema'
        }
    )

    fetch_write_versions = PythonOperator(
        task_id='fetch_and_write_latest_versions',
        python_callable=fetch_and_write_latest_versions,
    )


    # Dummy start and end tasks
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')



    with TaskGroup(group_id='process_entities') as process_entities_group:
        create_dynamic_tasks()


    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command=f'{ENTRYPOINT_CMD} && dbt deps',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
    )

    dbt_run_stg_cosential_opportunities = BashOperator(
        task_id='dbt_run_stg_cosential_opportunities',
        bash_command=f'{ENTRYPOINT_CMD} && dbt run -s stg_cosential_opportunities || (echo "ENTRYPOINT_CMD output: $(ENTRYPOINT_CMD)" && echo "dbt output: $(dbt run -s stg_cosential_opportunities)" && exit 1)',
        env={"PATH_TO_DBT_VENV": PATH_TO_DBT_VENV},
        cwd=PATH_TO_DBT_PROJECT,
        )



    # Set dependencies

    start_task >> [get_entities_task, get_schema_task, get_versions_task, get_entity_arrays_task] >> process_entities_group >> fetch_write_versions >> dbt_deps >> dbt_run_stg_cosential_opportunities >> end_task
