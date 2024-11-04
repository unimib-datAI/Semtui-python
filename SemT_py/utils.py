import zipfile
import os
import inspect
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
    
    @staticmethod
    def explore_class_methods(cls) -> List[str]:
        """
        Explore all methods of a class, filtering for user-defined functions only.
        """
        # List all methods defined in the class
        return [name for name, func in inspect.getmembers(cls, inspect.isfunction)]

    @staticmethod
    def explore_submodules(submodules: List) -> Dict[str, Dict[str, List[str]]]:
        """
        Explore all classes in the given submodules and list their functions.
        Args:
            submodules: A list of submodule objects to explore.
            
        Returns:
            A dictionary with the structure:
            {
                'module_name': {
                    'ClassName1': [method1, method2],
                    'ClassName2': [method1, method2]
                },
                ...
            }
        """
        result = {}

        for module in submodules:
            print(f"\nExploring module: {module.__name__}")
            print("-" * 60)

            # Get all classes defined in the module
            classes = [name for name, obj in inspect.getmembers(module, inspect.isclass) if obj.__module__ == module.__name__]
            
            module_dict = {}
            for cls_name in classes:
                cls = getattr(module, cls_name)
                methods = Utility.explore_class_methods(cls)
                module_dict[cls_name] = methods

                # Print results for the user
                print(f"Class: {cls_name}")
                print(f"  Methods: {methods}")
            
            result[module.__name__] = module_dict

        return result

    def push_to_backend(self, dataset_id: str, table_id: str, payload: Dict, debug: bool = False) -> Tuple[str, Dict]:
        """
        Pushes the payload data to the backend API
    
        Args:
            dataset_id (str): ID of the dataset
            table_id (str): ID of the table
            payload (Dict): The payload to be sent to the backend
            debug (bool): Flag to enable logging
    
        Returns:
            Tuple[str, Dict]: (success_message, payload)
        """
        def send_request(data: Dict, url: str) -> requests.Response:
            try:
                response = requests.put(url, json=data, headers=self.headers, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                if debug:
                    print(f"Request failed: {str(e)}")
                return None
    
        # Log payload if enabled
        if debug:
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
        if debug:
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

    def download_json(self, dataset_id: str, table_id: str, output_file: str = "downloaded_data.json") -> str:
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

    def parse_json(self, json_data: List[Dict]) -> pd.DataFrame:
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
    def display_json_table(json_table, number_of_rows=None, from_row=0, labels=None):
        """
        Displays a formatted HTML table from a JSON-based table structure with optional metadata.

        Parameters:
        -----------
        json_table : dict
            The JSON structure containing the table data. It must have two main keys:
            - 'columns': A dictionary where each key is a column label and the value is metadata.
            - 'rows': A dictionary where each key is a row identifier (e.g., 'r0', 'r1') and the value contains
            cell data for that row under the 'cells' key.

        number_of_rows : int, optional
            The number of rows to display starting from `from_row`. If not provided, all rows are displayed by default.
            
        from_row : int, optional
            The row index to start displaying from (default is 0).

        labels : list of str, optional
            A list of column labels to display. If not provided, all columns from `json_table['columns']` are used.

        Returns:
        --------
        IPython.core.display.HTML
            An HTML-formatted table as a string with structured metadata rendered, ready for display within Jupyter or any environment that supports HTML rendering.

        Behavior:
        ---------
        - Extracts the relevant rows and columns from the input `json_table`.
        - Displays metadata (if present) in each cell by structuring it in HTML format.
        - If metadata exists for a column, a new column is created (`<column>_metadata`) containing a formatted string of metadata.
        - Displays the table using `pandas.DataFrame.to_html` with HTML styling for better readability (auto-resizing, word wrapping).
        - If no rows or labels are provided, defaults to showing all rows and columns.

        Notes:
        ------
        - This function is useful when dealing with tables that have associated metadata in each cell. 
        - HTML rendering makes it easier to visualize the structured data, especially when working in environments like Jupyter notebooks.
        - Metadata for each cell, if present, includes fields like `ID`, `Name`, `Score`, `Match`, and `Type` where available, with clickable URLs where applicable.

        Example:
        --------
        json_table = {
            'columns': {
                'Name': {}, 'Age': {}, 'Occupation': {}
            },
            'rows': {
                'r0': {'cells': {'Name': {'label': 'Alice', 'metadata': [{'id': 1, 'name': 'Alice Smith', 'score': 98}]}}, 'Age': {'label': '29'}, 'Occupation': {'label': 'Engineer'}},
                'r1': {'cells': {'Name': {'label': 'Bob', 'metadata': [{'id': 2, 'name': 'Bob Lee', 'score': 85}]}}, 'Age': {'label': '35'}, 'Occupation': {'label': 'Doctor'}}
            }
        }

        display_json_table(json_table, number_of_rows=2, labels=['Name', 'Occupation'])

    """
        # Set default number_of_rows if not provided
        if number_of_rows is None:
            number_of_rows = len(json_table['rows'])  # Show all rows by default

        # Set default labels if not provided (use all column labels)
        if labels is None:
            labels = list(json_table['columns'].keys())

        # Extracting rows and creating a DataFrame
        data = []
        columns_with_metadata = set()  # To keep track of columns that have metadata

        for i in range(from_row, from_row + number_of_rows):
            row_key = f'r{i}'
            if row_key not in json_table['rows']:
                continue

            row_value = json_table['rows'][row_key]
            row_data = {}
            
            # Iterate over the selected labels (columns)
            for label in labels:
                cell_data = row_value['cells'].get(label, {})
                cell_label = cell_data.get('label', 'N/A')
                cell_metadata = cell_data.get('metadata', [])

                row_data[label] = cell_label

                if cell_metadata:
                    columns_with_metadata.add(label)
                    # Structuring metadata as a formatted string for display using HTML
                    formatted_metadata = []
                    for meta in cell_metadata:
                        metadata_lines = [
                            f"<strong>ID:</strong> {meta.get('id', 'N/A')}<br>"
                        ]
                        if 'name' in meta:
                            if isinstance(meta['name'], dict):
                                metadata_lines.extend([
                                    f"<strong>Name:</strong> {meta['name'].get('value', 'N/A')}<br>",
                                    f"<strong>URI:</strong> <a href='{meta['name'].get('uri', '#')}'>{meta['name'].get('uri', 'N/A')}</a><br>"
                                ])
                            else:
                                metadata_lines.append(f"<strong>Name:</strong> {meta['name']}<br>")
                        
                        metadata_lines.extend([
                            f"<strong>Score:</strong> {meta.get('score', 'N/A')}<br>",
                            f"<strong>Match:</strong> {meta.get('match', 'N/A')}<br>"
                        ])
                        
                        if 'type' in meta and isinstance(meta['type'], list):
                            types = ', '.join(t.get('name', 'N/A') for t in meta['type'])
                            metadata_lines.append(f"<strong>Types:</strong> {types}")
                        
                        formatted_metadata.append("".join(metadata_lines))

                    # Combine all metadata entries into one string with double line breaks between them
                    formatted_metadata_str = "<br><br>".join(formatted_metadata)
                    row_data[f'{label}_metadata'] = formatted_metadata_str

            data.append(row_data)

        # Creating DataFrame
        df = pd.DataFrame(data)

        # Remove metadata columns for labels that don't have metadata
        columns_to_keep = labels + [f'{label}_metadata' for label in columns_with_metadata]
        df = df[columns_to_keep]

        # Displaying the DataFrame as a table with structured metadata using HTML rendering
        pd.set_option('display.max_colwidth', None)  # Allow full display of cell contents

        # Define CSS styles for better formatting
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
            }}
            td {{
                max-width: 300px;
                word-wrap: break-word;
                white-space: pre-wrap;
            }}
        </style>
        {html_output}
        """

        # Render the styled HTML output
        return HTML(styled_output)
