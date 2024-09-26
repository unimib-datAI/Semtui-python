import zipfile
import os
import tempfile
import pandas as pd
import requests
import json
from urllib.parse import urljoin
from typing import Dict, Tuple, List, Optional
from .token_manager import TokenManager
from IPython.core.display import HTML

class Utility:
    def __init__(self, api_url: str, token_manager: TokenManager):
        self.api_url = api_url.rstrip('/') + '/'
        self.token_manager = token_manager
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        return {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.token_manager.get_token()}'
        }
    
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
        backend_url = urljoin(self.api_url, f"api/dataset/{dataset_id}/table/{table_id}")
        response = send_request(payload, backend_url)
    
        # Prepare output
        if response and response.status_code == 200:
            success_message = f"Updated Table successfully pushed to backend for table {table_id} in dataset {dataset_id}"
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
    
    def download_csv(self, dataset_id: str, table_id: str, output_file: str = "downloaded_data.csv") -> str:
        """
        Downloads a CSV file from the backend and saves it locally.
        Args:
            dataset_id (str): The ID of the dataset as a string.
            table_id (str): The ID of the table as a string.
            output_file (str): The name of the file to save the CSV data to. Defaults to "downloaded_data.csv".
        Returns:
            str: The path to the downloaded CSV file.
        """
        endpoint = f"/api/dataset/{dataset_id}/table/{table_id}/export"
        params = {"format": "csv"}
        url = urljoin(self.api_url, endpoint)

        response = requests.get(url, params=params, headers=self.headers)

        if response.status_code == 200:
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(response.text)
            print(f"CSV file has been downloaded successfully and saved as {output_file}")
            return output_file
        else:
            raise Exception(f"Failed to download CSV. Status code: {response.status_code}")

    def download_w3c_json(self, dataset_id: str, table_id: str, output_file: str = "downloaded_data.json") -> str:
        """
        Downloads a JSON file in W3C format from the backend and saves it locally.

        Args:
            dataset_id (str): The ID of the dataset as a string.
            table_id (str): The ID of the table as a string.
            output_file (str): The name of the file to save the JSON data to. Defaults to "downloaded_data.json".

        Returns:
            str: The path to the downloaded JSON file.
        """
        endpoint = f"/api/dataset/{dataset_id}/table/{table_id}/export"
        params = {"format": "w3c"}
        url = urljoin(self.api_url, endpoint)

        response = requests.get(url, params=params, headers=self.headers)

        if response.status_code == 200:
            # Parse the JSON data
            data = response.json()
            
            # Save the JSON data to a file
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"W3C JSON file has been downloaded successfully and saved as {output_file}")
            return output_file
        else:
            raise Exception(f"Failed to download W3C JSON. Status code: {response.status_code}")

    def parse_w3c_json(self, json_data: List[Dict]) -> pd.DataFrame:
        """
        Parses the W3C JSON format into a pandas DataFrame.

        Args:
            json_data (List[Dict]): The W3C JSON data.

        Returns:
            pd.DataFrame: A DataFrame containing the parsed data.
        """
        # Extract column names from the first item (metadata)
        columns = [key for key in json_data[0].keys() if key.startswith('th')]
        column_names = [json_data[0][col]['label'] for col in columns]

        # Extract data rows
        data_rows = []
        for item in json_data[1:]:  # Skip the first item (metadata)
            row = [item[col]['label'] for col in column_names]
            data_rows.append(row)

        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=column_names)
        return df
    
    @staticmethod
    def create_temp_csv(table_data: pd.DataFrame) -> str:
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
    def create_zip_file(self, df: pd.DataFrame, zip_filename: Optional[str] = None) -> str:
        """
        Creates a zip file containing a CSV file from the given DataFrame.
        The zip file is created as a temporary file unless a filename is specified.

        Args:
            df (pandas.DataFrame): The DataFrame to be saved as a CSV file.
            zip_filename (str, optional): The path to the zip file to be created.

        Returns:
            str: The path to the created zip file.
        """
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save the DataFrame to a CSV file in the temporary directory
            csv_file = os.path.join(temp_dir, 'data.csv')
            df.to_csv(csv_file, index=False)

            # Determine the path for the zip file
            if zip_filename:
                zip_path = zip_filename
            else:
                # Create a temporary file for the zip
                temp_zip = tempfile.NamedTemporaryFile(delete=False, suffix='.zip')
                temp_zip.close()
                zip_path = temp_zip.name

            # Create a zip file containing the CSV
            with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as z:
                z.write(csv_file, os.path.basename(csv_file))

            # Return the path to the zip file
            return zip_path
    @staticmethod
    def display_json_table(table_data, number_of_rows=None, from_row=0, labels=None):
        if number_of_rows is None:
            number_of_rows = len(table_data['rows'])

        if labels is None:
            labels = list(table_data['columns'].keys())

        data = []
        for i in range(from_row, from_row + number_of_rows):
            row_key = f'r{i}'
            if row_key not in table_data['rows']:
                continue

            row_value = table_data['rows'][row_key]
            row_data = {}

            for label in labels:
                cell = row_value['cells'].get(label, {})
                cell_label = cell.get('label', 'N/A')
                cell_metadata = cell.get('metadata', [])

                formatted_metadata = []
                for meta in cell_metadata:
                    metadata_lines = [f"<strong>{k}:</strong> {v}<br>" for k, v in meta.items() if k != 'type']
                    if 'type' in meta:
                        metadata_lines.append(f"<strong>Types:</strong> {', '.join(t.get('name', '') for t in meta['type'])}<br>")
                    formatted_metadata.append("".join(metadata_lines))

                formatted_metadata_str = "<br><br>".join(formatted_metadata) if formatted_metadata else 'No Metadata'
                
                row_data[label] = cell_label
                row_data[f'{label}_metadata'] = formatted_metadata_str

            data.append(row_data)

        df = pd.DataFrame(data)
        pd.set_option('display.max_colwidth', None)

        html_output = df.to_html(escape=False, index=False)
        styled_output = f"""
        <style>
            table {{
                width: 100%;
                border-collapse: collapse;
            }}
            th, td {{
                padding: 8px;
                text-align: left;
                vertical-align: top;
                border: 1px solid #ddd;
            }}
            td {{
                max-width: 300px;
                word-wrap: break-word;
                white-space: pre-wrap;
            }}
            tr:nth-child(even) {{
                background-color: #f2f2f2;
            }}
            th {{
                background-color: #4CAF50;
                color: white;
            }}
        </style>
        {html_output}
        """
        return HTML(styled_output)

