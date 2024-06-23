import requests
from requests.auth import HTTPBasicAuth
from plugins.azure_utils import get_secret
import os

# Pull the environment variables
cos_username = os.getenv('COSENTIAL_USER')
firm_id = os.getenv('COSENTIAL_FIRM_ID')
cosential_pw_secret_name = os.getenv('COSENTIAL_PW_SECRET')
cosential_api_key_secret_name = os.getenv('COSENTIAL_APIKEY_SECRET')

# Get the secret values
cosential_pw = get_secret(cosential_pw_secret_name)
cosential_api_key = get_secret(cosential_api_key_secret_name)



def make_api_call(relative_url):
    """
    Helper function to make an API call.

    Args:
        relative_url (str): The relative URL for the API endpoint.

    Returns:
        list: The list of JSON responses from the API call.
    """
    base_url = "https://compass.cosential.com/"

    api_endpoint = base_url + relative_url

    headers = {
        'x-compass-firm-id': firm_id,
        'x-compass-api-key': cosential_api_key,
        'Content-Type': 'application/json'
    }

    try:
        aggregated_data = []  # List to hold all response data

        # Initialize paging parameters
        size = 500
        from_value = 0
        total_records = 0

        while True:
            # Set paging parameters in the API call
            params = {
                'SIZE': size,
                'FROM': from_value
            }

            response = requests.get(
                api_endpoint,
                auth=HTTPBasicAuth(cos_username, cosential_pw),
                headers=headers,
                params=params
            )
            response.raise_for_status()

            # Append response data to aggregated_data
            aggregated_data.extend(response.json())

            # Update total records and check if there are more pages
            total_records += len(response.json())
            if len(response.json()) < size:
                break

            # Increment from_value for the next page
            from_value += size

        return aggregated_data

    except Exception as e:
        print(f"An error occurred while making the API call to {api_endpoint}: {str(e)}")
        return None



def fetch_changed_ids(entity_endpoint, versions):
    """
    Fetches the changed IDs for the given entity endpoint.

    Args:
        entity_endpoint (dict): A dictionary containing the entity endpoint and name.
        cosential_pw (str): The password secret for authentication.
        cosential_api_key (str): The API key secret for authentication.
        versions (dict): A dictionary containing the latest versions for each entity.

    Returns:
        dict: A dictionary containing the changed IDs for the entity.
    """

    changed_ids = {}  # Create an empty dictionary to store the changed ids

    endpoint = entity_endpoint["Endpoint"]
    entity = entity_endpoint["Entity"]
    latest_version = versions.get(entity)  # Get the latest version for the entity

    if latest_version is not None:
        relative_url = f"{endpoint}/changes?version={latest_version}&includeDeleted=true&reverse=true"

        try:
            changed_data = make_api_call(relative_url)   # Get the changed data from the response

            # Extract the Id values from the changed data
            changed_ids[entity] = [item["Id"] for item in changed_data]
            print(f"Changed Ids for {entity}: {changed_ids[entity]}")

        except Exception as e:
            print(f"An error occurred while getting changed Ids for {entity}: {str(e)}")

    return changed_ids


def update_latest_version(entities):
    """
    Updates the latest version for the given entities.

    Args:
        entities (list): A list of dictionaries containing the entity endpoints and names.
        cosential_pw (str): The password secret for authentication.
        cosential_api_key (str): The API key secret for authentication.

    Returns:
        dict: A dictionary containing the latest versions for each entity.
    """
    all_versions = {}  # Create an empty dictionary to store all versions

    for entity in entities:
        endpoint = entity["Endpoint"]
        relative_url= f"{endpoint}/changes?reverse=true"
        entity_name = entity["Entity"]

        # Custom headers


        try:
            response = make_api_call(relative_url)  # Make the API call to get the latest version
            latest_version = response.json()[0]["Version"]  # Use index 0 to access the first item in the response

            all_versions[entity_name] = latest_version  # Add the latest version to the dictionary

        except Exception as e:
            print(f"An error occurred while getting latest version for {entity_name}: {str(e)}")

    return all_versions


def pull_entity_objects(entity_endpoint, object_ids):
    """
    Pulls the objects for a given entity endpoint.

    Args:
        entity_endpoint (dict): A dictionary containing the entity endpoint and name.
        object_ids (dict): A dictionary containing the object IDs for the entity.
        cosential_pw (str): The password secret for authentication.
        cosential_api_key (str): The API key secret for authentication.

    Returns:
        list: A list of dictionaries containing the data for the specified entity objects.
    """

    endpoint = entity_endpoint["Endpoint"]
    entity = entity_endpoint["Entity"]

    if entity not in object_ids or not object_ids[entity]:
        return []  # Return an empty list if there are no object_ids for this entity

    aggregated_data = []  # List to hold all objects for the current entity

    for object_id in object_ids[entity]:
        relative_url = f"{endpoint}/{object_id}"

        # Make the API call and return response
        try:
            response = make_api_call(relative_url)

            # Append response data to aggregated_data
            aggregated_data.append(response.json())

        except Exception as e:
            print(f"An error occurred while pulling data for {entity} with ID {object_id}: {str(e)}")
    return aggregated_data



def pull_entity_arrays(entity, array_name, object_ids):
    """
    Pulls the objects for a given entity endpoint.

    Args:
        entity (dict): A dictionary containing the entity endpoint and name.
        array_name (str): The name of the array to pull.
        object_ids (dict): A dictionary containing the object IDs for the entity.
        cosential_pw (str): The password secret for authentication.
        cosential_api_key (str): The API key secret for authentication.

    Returns:
        list: A list of dictionaries containing the data for the specified entity objects.
    """
    endpoint = entity["Endpoint"]
    entity_name = entity["Entity"]

    if entity_name not in object_ids or not object_ids[entity_name]:
        return []  # Return an empty list if there are no object_ids for this entity

    aggregated_data = []  # List to hold all objects for the current entity

    for object_id in object_ids[entity_name]:
        relative_url = f"{endpoint}/{object_id}/{array_name}"
        # Make the API call and return response
        try:
            response = make_api_call(relative_url)

            # Append response data to aggregated_data
            response_data = response.json()

            if isinstance(response_data, dict):
                response_data["ObjectId"] = object_id
                aggregated_data.append(response_data)
            elif isinstance(response_data, list):
                for item in response_data:
                    if isinstance(item, dict):
                        item["ObjectId"] = object_id
                aggregated_data.extend(response_data)
            else:
                print(f"Unexpected response data type for {entity_name} with ID {object_id}: {type(response_data)}")
                print(response_data)

        except requests.exceptions.RequestException as e:
            print(f"An error occurred while pulling data for {entity_name} with ID {object_id}: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred while processing data for {entity_name} with ID {object_id}: {str(e)}")

    return aggregated_data


def pull_all_entities(entity_endpoints):
    """
    Pulls data for multiple entities from their respective endpoints.

    Args:
        entity_endpoints (list): A list of dictionaries containing the endpoint and entity information for each entity.

    Returns:
        list: A list of all objects for all entities.

    Raises:
        Exception: If an error occurs while pulling data for any entity.
    """

    aggregated_data = []  # List to hold all objects for all entities

    for entity_endpoint in entity_endpoints:
        endpoint = entity_endpoint["Endpoint"]
        entity = entity_endpoint["Entity"]
        relative_url = f"{endpoint}"

        # Make the API call and return response
        try:
            response_data = make_api_call(relative_url)
            aggregated_data.extend(response_data)

        except Exception as e:
            print(f"An error occurred while pulling data for {entity}: {str(e)}")
            return False
    return aggregated_data
# Path: dags/scripts/pull_entities.py