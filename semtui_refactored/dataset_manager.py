import requests
import json
import pandas as pd
import os 
from .utils import Utility  # Ensure the Utility class is imported correctly
from .token_manager import TokenManager
from urllib.parse import urljoin
from fake_useragent import UserAgent

class DatasetManager:
    def __init__(self, api_url, token_manager):
        self.api_url = api_url.rstrip('/') + '/'
        self.token_manager = token_manager
        self.user_agent = UserAgent()

    def _get_headers(self):
        token = self.token_manager.get_token()
        return {
            'Accept': 'application/json, text/plain, */*',
            'Authorization': f'Bearer {token}',
            'User-Agent': self.user_agent.random,
            'Origin': self.api_url.rstrip('/'),
            'Referer': self.api_url
        }

    def get_database_list(self):
        """
        Retrieves the list of datasets from the server.

        Returns:
            DataFrame: A DataFrame containing datasets information.
        """
        url = f"{self.api_url}api/dataset"
        headers = self._get_headers()
        
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
            print(f"Response status code: {response.status_code}")
            print(f"Response content: {response.text[:200]}...")  # Print first 200 characters
            
            data = response.json()
            
            if 'collection' in data:
                df = pd.DataFrame(data['collection'])
                print(f"Retrieved {len(df)} datasets")
                return df
            else:
                print("Unexpected response structure. 'collection' key not found.")
                print(f"Keys in response: {data.keys()}")
                return None

        except requests.RequestException as e:
            print(f"Request failed: {e}")
            if hasattr(e, 'response'):
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text}")
            return None

        except ValueError as e:
            print(f"JSON decoding failed: {e}")
            return None

    def delete_dataset(self, dataset_id):
        """
        Deletes a specific dataset from the server using the specified API endpoint.

        Args:
            dataset_id (str): The unique identifier of the dataset to be deleted.

        Returns:
            str: A message indicating the result of the operation.
        """
        # Construct the full URL for the DELETE request
        url = f"{self.api_url}dataset/{dataset_id}"

        # Send the DELETE request to remove the dataset
        response = requests.delete(url, headers=self.headers)

        # Check the response and return appropriate messages
        if response.status_code == 200:
            return f"Dataset with ID {dataset_id} deleted successfully!"
        elif response.status_code == 401:
            return "Unauthorized: Invalid or missing token."
        elif response.status_code == 404:
            return f"Dataset with ID {dataset_id} not found."
        else:
            return f"Failed to delete dataset: {response.status_code}, {response.text}"

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
            url = f"{self.api_url}dataset/{dataset_id}"

            # Send the DELETE request to remove the dataset
            response = requests.delete(url, headers=self.headers)

            # Check the response and append appropriate messages
            if response.status_code == 200:
                results.append(f"Dataset with ID {dataset_id} deleted successfully!")
            elif response.status_code == 401:
                results.append(f"Unauthorized: Invalid or missing token for dataset ID '{dataset_id}'.")
            elif response.status_code == 404:
                results.append(f"Dataset with ID {dataset_id} not found.")
            else:
                results.append(f"Failed to delete dataset with ID '{dataset_id}': {response.status_code}, {response.text}")
        
        return results

    def rename_dataset(self, dataset_id, new_name):
        """
        Renames a specific dataset on the server using the specified API endpoint.

        Args:
            dataset_id (str): The unique identifier of the dataset to be renamed.
            new_name (str): The new name for the dataset.

        Returns:
            str: A message indicating the result of the operation.
        """
        # Construct the full URL for the PUT request
        url = f"{self.api_url}dataset/{dataset_id}"
        headers = {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        payload = {
            "name": new_name
        }

        # Send the PUT request to rename the dataset
        response = requests.put(url, headers=headers, json=payload)

        # Check the response and return appropriate messages
        if response.status_code == 200:
            return f"Dataset with ID {dataset_id} renamed to '{new_name}' successfully!"
        elif response.status_code == 401:
            return "Unauthorized: Invalid or missing token."
        elif response.status_code == 404:
            return f"Dataset with ID {dataset_id} not found."
        else:
            return f"Failed to rename dataset: {response.status_code}, {response.text}"

    def get_dataset_tables(self, dataset_id):
        """
        Retrieves the list of tables for a given dataset.

        Args:
            dataset_id (str): The ID of the dataset.

        Returns:
            list: A list of tables in the dataset.
        """
        try:
            url = f"{self.api_url}dataset/{dataset_id}/table"
            headers = {
                "accept": "application/json",
                "authorization": f"Bearer {self.token_manager.get_token()}"
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception if the request was not successful
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
        headers = {
            "Authorization": f"Bearer {self.token_manager.get_token()}",
            "Accept": "application/json"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Raise an exception if the request was not successful
            table_data = response.json()

            return table_data

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
                    table_data["id"] = table_id  # Ensure the ID is included in the returned data
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
            table_data["id"] = table_id  # Ensure the ID is included in the returned data
            return table_data
        
        print(f"Table with ID '{table_id}' not found in the dataset.")
        return None

    def add_table_to_dataset(self, dataset_id, table_data, table_name):
        """
        Adds a table to a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_data (DataFrame): The table data to be added.
            table_name (str): The name of the table to be added.
        """
        url = f"{self.api_url}dataset/{dataset_id}/table/"
        headers = {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Accept': 'application/json'
        }
        
        # Create a temporary CSV file from the DataFrame
        temp_file_path = Utility.create_temp_csv(table_data)
        
        try:
            with open(temp_file_path, 'rb') as file:
                files = {
                    'file': (file.name, file, 'text/csv')
                }
                
                data = {
                    'name': table_name
                }
                
                response = requests.post(url, headers=headers, data=data, files=files, timeout=30)
            
            if response.status_code in [200, 201]:
                print("Table added successfully!")
                response_data = response.json()
                if 'tables' in response_data:
                    tables = response_data['tables']
                    for table in tables:
                        table_id = table['id']
                        table_name = table['name']
                        print(f"New table added: ID: {table_id}, Name: {table_name}")
                else:
                    print("Response JSON does not contain 'tables' key.")
            else:
                print(f"Failed to add table: {response.status_code}, {response.text}")
        
        except requests.RequestException as e:
            print(f"Request error occurred: {e}")
        
        except IOError as e:
            print(f"File I/O error occurred: {e}")
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def update_table_in_dataset(self, dataset_id, table_id, table_data, table_name):
        """
        Updates an existing table in a specific dataset.

        Args:
            dataset_id (str): The ID of the dataset.
            table_id (str): The ID of the table to be updated.
            table_data (DataFrame): The new table data.
            table_name (str): The new name for the table.

        Returns:
            str: A message indicating the result of the operation.
        """
        url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
        headers = {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Accept': 'application/json'
        }
        
        # Create a temporary CSV file from the DataFrame
        temp_file_path = Utility.create_temp_csv(table_data)
        
        try:
            with open(temp_file_path, 'rb') as file:
                files = {
                    'file': (file.name, file, 'text/csv')
                }
                
                data = {
                    'name': table_name
                }
                
                response = requests.put(url, headers=headers, data=data, files=files, timeout=30)
            
            if response.status_code == 200:
                print("Table updated successfully!")
                response_data = response.json()
                if 'table' in response_data:
                    table_id = response_data['table']['id']
                    table_name = response_data['table']['name']
                    print(f"Updated table: ID: {table_id}, Name: {table_name}")
                else:
                    print("Response JSON does not contain 'table' key.")
            else:
                print(f"Failed to update table: {response.status_code}, {response.text}")
        
        except requests.RequestException as e:
            print(f"Request error occurred: {e}")
        
        except IOError as e:
            print(f"File I/O error occurred: {e}")
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)

    def rename_table(self, dataset_id, table_id, new_name):
        """
        Renames a specific table in a dataset on the server using the specified API endpoint.

        Args:
            dataset_id (str): The unique identifier of the dataset containing the table to be renamed.
            table_id (str): The unique identifier of the table to be renamed.
            new_name (str): The new name for the table.

        Returns:
            str: A message indicating the result of the operation.
        """
        # Construct the full URL for the PUT request
        url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
        headers = {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        payload = {
            "name": new_name
        }

        # Send the PUT request to rename the table
        response = requests.put(url, headers=headers, json=payload)

        # Check the response and return appropriate messages
        if response.status_code == 200:
            return f"Table with ID {table_id} renamed to '{new_name}' successfully!"
        elif response.status_code == 401:
            return "Unauthorized: Invalid or missing token."
        elif response.status_code == 404:
            return f"Table with ID {table_id} not found in the dataset."
        else:
            return f"Failed to rename table: {response.status_code}, {response.text}"

    def update_table(self, dataset_id, table_name, update_payload):
        """
        Updates a table in a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_name (str): The name of the table to update.
            update_payload (dict): The payload containing the updated table data.
        
        Returns:
            None
        """
        tables = self.get_dataset_tables(dataset_id)
        
        for table in tables:
            if table["name"] == table_name:
                table_id = table["id"]
                url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
                headers = {
                    "Authorization": f"Bearer {self.token_manager.get_token()}",
                    "Content-Type": "application/json"
                }
                
                try:
                    response = requests.put(url, headers=headers, json=update_payload)
                    
                    if response.status_code == 200:
                        print("Table updated successfully!")
                        response_data = response.json()
                        print("Response data:", response_data)
                    elif response.status_code == 401:
                        print("Unauthorized: Invalid or missing token.")
                    elif response.status_code == 404:
                        print(f"Dataset or table with ID {dataset_id}/{table_id} not found.")
                    else:
                        print(f"Failed to update table: {response.status_code}, {response.text}")
                except requests.exceptions.RequestException as e:
                    print(f"Error occurred while updating table: {e}")
                
                return
        
        print(f"Table '{table_name}' not found in the dataset.")

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
        headers = {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Accept': 'application/json'
        }
        
        response = requests.delete(url, headers=headers)
        
        if response.status_code == 200:
            print(f"Table '{table_name}' deleted successfully!")
        elif response.status_code == 401:
            print("Unauthorized: Invalid or missing token.")
        elif response.status_code == 404:
            print(f"Table '{table_name}' not found in the dataset.")
        else:
            print(f"Failed to delete table: {response.status_code}, {response.text}")
        
    def delete_tables_by_id(self, dataset_id, table_ids):
        """
        Deletes multiple tables by their IDs from a specific dataset.
        
        Args:
            dataset_id (str): The ID of the dataset.
            table_ids (list): A list of table IDs to delete.
        """
        headers = {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Accept': 'application/json'
        }

        for table_id in table_ids:
            url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
            
            response = requests.delete(url, headers=headers)
            
            if response.status_code == 200:
                print(f"Table with ID '{table_id}' deleted successfully!")
            elif response.status_code == 401:
                print(f"Unauthorized: Invalid or missing token for table ID '{table_id}'.")
            elif response.status_code == 404:
                print(f"Table with ID '{table_id}' not found in the dataset.")
            else:
                print(f"Failed to delete table with ID '{table_id}': {response.status_code}, {response.text}")

    def get_database_list(self):
        """
        Retrieves the list of datasets from the server.

        Returns:
            DataFrame: A DataFrame containing datasets information.
        """
        url = f"{self.api_url}dataset/"
        headers = {
            "accept": "application/json",
            "authorization": f"Bearer {self.token_manager.get_token()}"
        }
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()  # Will handle HTTP errors
            database_list = response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            return None  # Return None or raise an exception instead of an empty DataFrame

        # Initialize data dictionary with keys and empty lists
        data = {key: [] for key in ['id', 'userId', 'name', 'nTables', 'lastModifiedDate']}

        for dataset in database_list.get('collection', []):  # Safe default if 'collection' key is missing
            for key in data:
                data[key].append(dataset.get(key, None))  # None if key doesn't exist in dataset

        return pd.DataFrame(data)