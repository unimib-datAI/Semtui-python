import requests
import json
import pandas as pd
import os
from urllib.parse import urljoin
from fake_useragent import UserAgent
from .utils import Utility 
from .token_manager import TokenManager
from typing import TYPE_CHECKING, List, Optional, Tuple, Dict, Any
import logging
from requests.exceptions import RequestException, JSONDecodeError

# Configure logging
#logging.basicConfig(level=logging.INFO)
#logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .token_manager import TokenManager


class DatasetManager:
    def __init__(self, base_url, token_manager):
        self.base_url = base_url.rstrip('/') + '/'
        self.api_url = urljoin(self.base_url, 'api/')
        self.token_manager = token_manager
        self.user_agent = UserAgent()
        self.logger = logging.getLogger(__name__)

    def _get_headers(self):
        token = self.token_manager.get_token()
        return {
            'Accept': 'application/json, text/plain, */*',
            'Authorization': f'Bearer {token}',
            'User-Agent': self.user_agent.random,
            'Origin': self.base_url.rstrip('/'),
            'Referer': self.base_url
        }
    
    def get_database_list(self, debug: bool = False) -> pd.DataFrame:
        """
        Retrieves the list of datasets from the server.

        Args:
            debug (bool): If True, prints additional information like metadata and status code.
            
        Returns:
            DataFrame: A DataFrame containing the datasets.
        """
        url = urljoin(self.api_url, 'dataset')
        headers = self._get_headers()
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            data = response.json()
            
            if debug:
                print(f"Status Code: {response.status_code}")
                print("Metadata:")
                print(json.dumps(data.get('meta', {}), indent=4))  # Display metadata in a pretty format
                
            if 'collection' in data:
                # Convert the 'collection' key into a DataFrame
                return pd.DataFrame(data['collection'])
            else:
                print("Unexpected response structure. 'collection' key not found.")
                return pd.DataFrame()  # Return an empty DataFrame if structure is not as expected

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text[:200]}...")
            return pd.DataFrame()

        except ValueError as e:
            print(f"JSON decoding failed: {e}")
            return pd.DataFrame()
    
    def delete_dataset(self, dataset_id):
        """
        Deletes a specific dataset from the server using the specified API endpoint.

        Args:
            dataset_id (str): The unique identifier of the dataset to be deleted.

        Returns:
            str: A message indicating the result of the operation.
        """
        url = f"{self.api_url}dataset/{dataset_id}"
        headers = self._get_headers()

        try:
            response = requests.delete(url, headers=headers)
            response.raise_for_status()
            return f"Dataset with ID {dataset_id} deleted successfully!"
        except requests.RequestException as e:
            if e.response.status_code == 401:
                return "Unauthorized: Invalid or missing token."
            elif e.response.status_code == 404:
                return f"Dataset with ID {dataset_id} not found."
            else:
                return f"Failed to delete dataset: {e.response.status_code}, {e.response.text}"

    def delete_datasets(self, dataset_ids):
        """
        Deletes multiple datasets by their IDs from the server using the specified API endpoint.
        
        Args:
            dataset_ids (list): A list of dataset IDs to delete.

        Returns:
            list: A list of messages indicating the result of each deletion operation.
        """
        results = []

        for dataset_id in dataset_ids:
            result = self.delete_dataset(dataset_id)
            results.append(result)
        
        return results
    
    def get_dataset_tables(self, dataset_id):
        """
        Retrieves the list of tables for a given dataset.

        Args:
            dataset_id (str): The ID of the dataset.

        Returns:
            list: A list of tables in the dataset.
        """
        url = f"{self.api_url}dataset/{dataset_id}/table"
        headers = self._get_headers()

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()["collection"]
        except (requests.RequestException, json.JSONDecodeError, KeyError) as e:
            print(f"Error getting dataset tables: {e}")
            return []

    def get_table(self, dataset_id, table_id):
        """
        Retrieves a table by its ID from a specific dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            table_id (str): The ID of the table to retrieve.

        Returns:
            dict: The table data in JSON format.
        """
        url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
        headers = self._get_headers()

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error occurred while retrieving the table data: {e}")
            return None

    def get_table_by_name(self, dataset_id, table_name):
        """
        Retrieves a table by its name from a specific dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the table to retrieve.

        Returns:
            dict: The table data in JSON format, including the table_id.
        """
        tables = self.get_dataset_tables(dataset_id)

        for table in tables:
            if table["name"] == table_name:
                table_id = table["id"]
                table_data = self.get_table(dataset_id, table_id)
                if table_data:
                    table_data["id"] = table_id
                    return table_data

        print(f"Table '{table_name}' not found in the dataset.")
        return None

    def get_table_by_id(self, dataset_id, table_id):
        """
        Retrieves a table by its ID from a specific dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            table_id (str): The ID of the table to retrieve.

        Returns:
            dict: The table data in JSON format, including the table_id.
        """
        table_data = self.get_table(dataset_id, table_id)
        if table_data:
            table_data["id"] = table_id
            return table_data
        
        print(f"Table with ID '{table_id}' not found in the dataset.")
        return None

    def _process_add_table_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(result, dict) and 'tables' in result:
            if result['tables'] and len(result['tables']) > 0:
                table = result['tables'][0]
                table_id = table.get('id')
                table_name = table.get('name')
                message = f"Table added successfully! New table added: ID: {table_id}, Name: {table_name}"
                return {
                    'message': message,
                    'table_id': table_id,
                    'response_data': result
                }
            else:
                return {
                    'message': "No tables found in the response.",
                    'response_data': result
                }
        else:
            return {
                'message': "Unexpected response format.",
                'response_data': result
            }

    def add_table_to_dataset(self, dataset_id: str, table_data: pd.DataFrame, table_name: str) -> (str, Dict[str, Any]):
        """
        Adds a table to a specific dataset and processes the result.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_data (DataFrame): The table data to be added.
            table_name (str): The name of the table to be added.
        
        Returns:
            tuple: A tuple containing:
                - message (str): A descriptive message about the operation.
                - response_data (dict): The full response data from the API.
        """
        url = f"{self.api_url}dataset/{dataset_id}/table/"
        headers = self._get_headers()
        headers.pop('Content-Type', None)  # Remove Content-Type for file upload
        
        temp_file_path = Utility.create_temp_csv(table_data)
        
        try:
            with open(temp_file_path, 'rb') as file:
                files = {'file': (file.name, file, 'text/csv')}
                data = {'name': table_name}
                
                response = requests.post(url, headers=headers, data=data, files=files, timeout=30)
            
            response.raise_for_status()
            response_data = response.json()
            
            # Process the result
            result = self._process_add_table_result(response_data)
            
            if 'table_id' in result:
                self.logger.info(result['message'])
            else:
                self.logger.warning(result['message'])
            
            return result['message'], result['response_data']  # Return the message and response_data separately
        
        except requests.RequestException as e:
            error_message = f"Request error occurred: {str(e)}"
            if hasattr(e, 'response'):
                error_message += f"\nResponse status code: {e.response.status_code}"
                error_message += f"\nResponse content: {e.response.text[:200]}..."
            self.logger.error(error_message)
            return error_message, None
        
        except IOError as e:
            error_message = f"File I/O error occurred: {str(e)}"
            self.logger.error(error_message)
            return error_message, None
        
        except Exception as e:
            error_message = f"An unexpected error occurred: {str(e)}"
            self.logger.error(error_message)
            return error_message, None
        
        finally:
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def extract_table_id(self, result: Dict[str, Any]) -> str:
        """
        Extracts the table ID from the result of add_table_to_dataset.
        
        Args:
            result (dict): The result dictionary returned by add_table_to_dataset.
        
        Returns:
            str: The table ID if available, otherwise None.
        """
        if 'tables' in result and len(result['tables']) > 0:
            return result['tables'][0].get('id')
        return None
    
    def list_tables_in_dataset(self, dataset_id):
        """
        Lists all tables in a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
        """
        tables = self.get_dataset_tables(dataset_id)
        
        if not tables:
            print(f"No tables found in dataset with ID: {dataset_id}")
            return
        
        print(f"Tables in dataset {dataset_id}:")
        for table in tables:
            table_id = table.get('id')
            table_name = table.get('name')
            if table_id and table_name:
                print(f"ID: {table_id}, Name: {table_name}")
            else:
                print("A table with missing ID or name was found.")

    def delete_table(self, dataset_id, table_name):
        """
        Deletes a table by its name from a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the table to delete.
        """
        table = self.get_table_by_name(dataset_id, table_name)
        if not table:
            return
        
        table_id = table.get("id")
        if not table_id:
            print(f"Failed to retrieve the ID for table '{table_name}'.")
            return
        
        url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
        headers = self._get_headers()
        
        try:
            response = requests.delete(url, headers=headers)
            response.raise_for_status()
            print(f"Table '{table_name}' deleted successfully!")
        except requests.RequestException as e:
            if e.response.status_code == 401:
                print("Unauthorized: Invalid or missing token.")
            elif e.response.status_code == 404:
                print(f"Table '{table_name}' not found in the dataset.")
            else:
                print(f"Failed to delete table: {e.response.status_code}, {e.response.text}")

    def delete_tables_by_id(self, dataset_id, table_ids):
        """
        Deletes multiple tables by their IDs from a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_ids (list): A list of table IDs to delete.
        """
        headers = self._get_headers()

        for table_id in table_ids:
            url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
            
            try:
                response = requests.delete(url, headers=headers)
                response.raise_for_status()
                print(f"Table with ID '{table_id}' deleted successfully!")
            except requests.RequestException as e:
                if e.response.status_code == 401:
                    print(f"Unauthorized: Invalid or missing token for table ID '{table_id}'.")
                elif e.response.status_code == 404:
                    print(f"Table with ID '{table_id}' not found in the dataset.")
                else:
                    print(f"Failed to delete table with ID '{table_id}': {e.response.status_code}, {e.response.text}")
  