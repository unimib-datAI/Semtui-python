import zipfile
import os
import tempfile
import pandas as pd
import requests 
import json
from typing import Dict, Tuple
from .token_manager import TokenManager

class Utility:
    def __init__(self, api_url: str, token_manager: TokenManager):
        self.api_url = api_url
        self.token_manager = token_manager
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token_manager.get_token()}'
        }
    
    @staticmethod
    def create_zip_file(df, zip_filename):
        """
        Creates a zip file containing a CSV file from the given DataFrame.
        The zip file is created in the system's temporary directory.

        Args:
            df (pandas.DataFrame): The DataFrame to be saved as a CSV file.
            zip_filename (str): The name of the zip file to be created.

        Returns:
            str: The path to the created zip file.
        """
        # Create a temporary directory
        temp_dir = tempfile.mkdtemp()
        try:
            # Define the path for the CSV file
            csv_file = os.path.join(temp_dir, 'data.csv')
            # Save the DataFrame to a CSV file
            df.to_csv(csv_file, index=False)
            # Create a zip file containing the CSV
            zip_path = os.path.join(temp_dir, zip_filename)
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(csv_file, os.path.basename(csv_file))
            # Return the path to the created zip file
            return zip_path
        except Exception as e:
            # Clean up the temporary directory if an exception occurs
            os.remove(temp_dir)
            raise e

    @staticmethod
    def create_temp_csv(table_data):
        """
        Creates a temporary CSV file from a DataFrame.

        Args:
            table_data (DataFrame): The table data to be written to the CSV file.

        Returns:
            str: The path of the temporary CSV file.
        """
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.csv') as temp_file:
            table_data.to_csv(temp_file, index=False)
            temp_file_path = temp_file.name

        return temp_file_path

    @staticmethod
    def load_json_to_dataframe(data, georeference_data=False):
        """
        Converts a JSON object containing dataset information into a pandas DataFrame.

        This function assumes the JSON object has 'columns' and 'rows' keys. Each row is expected to
        contain 'cells', which are transformed into DataFrame columns. If georeference_data is True,
        additional columns for latitude and longitude will be included based on georeference metadata.

        Args:
            data (dict): A JSON-like dictionary containing the data. This dictionary should
                        have at least two keys: 'columns' and 'rows', where 'rows' should
                        be a dictionary of dictionaries containing cell data.
            georeference_data (bool): A flag to include georeference data (latitude and longitude) in the DataFrame.

        Returns:
            pd.DataFrame: A DataFrame where each row corresponds to entries in the 'rows' of the input JSON.
                        Each cell in 'rows' becomes a column in the DataFrame.

        Raises:
            KeyError: If the expected keys ('columns' or 'rows') are missing in the input data.
            Exception: For other issues that might occur during DataFrame creation.
        """
        try:
            # Extract columns and rows from the data
            columns = data['columns']  # This is extracted but not used, assuming future use cases
            rows = data['rows']

            # Initialize a list to store each row's data as a dictionary
            data_list = []

            for row_id, row_info in rows.items():
                row_data = {}
                city_label = ''
                city_uri = ''
                latitude = ''
                longitude = ''
                
                # Extract cell data into dictionary form, using the label as the value
                for cell_key, cell_value in row_info['cells'].items():
                    row_data[cell_key] = cell_value['label']
                    
                    # If georeference_data is True, extract georeference data
                    if georeference_data and cell_key == 'City':
                        city_cell = cell_value
                        city_label = city_cell.get('label', '')
                        for meta in city_cell.get('metadata', []):
                            if 'name' in meta and 'uri' in meta['name']:
                                city_uri = meta['name']['uri']
                                if 'georss' in meta['id']:
                                    coordinates = meta['id'].split(':')[1].split(',')
                                    latitude = coordinates[0]
                                    longitude = coordinates[1]
                                break
                
                if georeference_data:
                    row_data['City'] = city_label
                    row_data['City URI'] = city_uri
                    row_data['Latitude'] = latitude
                    row_data['Longitude'] = longitude

                data_list.append(row_data)

            # Convert the list of dictionaries to a pandas DataFrame
            df = pd.DataFrame(data_list)
            
            return df

        except KeyError as e:
            print(f"Key error: Missing {str(e)} in the data.")
            raise
        except Exception as e:
            print(f"An error occurred while converting JSON to DataFrame: {str(e)}")
            raise
    
    def push_to_backend(self, dataset_id: str, table_id: str, payload: Dict, enable_logging: bool = False) -> Tuple[str, Dict]:
        """
        Pushes the payload data to the backend API

        Args:
            dataset_id (str): ID of the dataset
            table_id (str): ID of the table
            payload (Dict): The payload to be sent to the backend
            enable_logging (bool): Flag to enable logging

        Returns:
            Tuple[str, Dict]: (success_message, payload)
        """
        def send_request(data: Dict, url: str) -> requests.Response:
            try:
                response = requests.put(url, json=data, headers=self.headers, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if enable_logging:
                    print(f"Request failed: {str(e)}")
                return None

        # Log payload if enabled
        if enable_logging:
            print("Payload being sent:")
            print(json.dumps(payload, indent=2))

        # Push to backend
        backend_url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"
        response = send_request(payload, backend_url)

        # Prepare output
        if response and response.status_code == 200:
            success_message = f"Extension successfully pushed to backend for table {table_id} in dataset {dataset_id}"
        else:
            status_code = response.status_code if response else "N/A"
            success_message = f"Failed to push to backend. Status code: {status_code}"

        # Log response if enabled
        if enable_logging:
            if response:
                print(f"Status Code: {response.status_code}")
                print(f"Response: {response.text}")
            else:
                print("No response received from the server.")

        return success_message, payload
