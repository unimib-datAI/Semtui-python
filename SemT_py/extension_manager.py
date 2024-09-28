import requests
import json
import copy
import pandas as pd
from urllib.parse import urljoin
from copy import deepcopy
from .token_manager import TokenManager

class ExtensionManager:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip('/') + '/'
        self.api_url = urljoin(self.base_url, 'api/extenders')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def create_backend_payload(self, reconciled_json):
        nCellsReconciliated = sum(
            1 for row in reconciled_json['rows'].values()
            for cell in row['cells'].values()
            if cell.get('annotationMeta', {}).get('annotated', False)
        )
        all_scores = [
            cell.get('annotationMeta', {}).get('lowestScore', float('inf'))
            for row in reconciled_json['rows'].values()
            for cell in row['cells'].values()
            if cell.get('annotationMeta', {}).get('annotated', False)
        ]
        minMetaScore = min(all_scores) if all_scores else 0
        maxMetaScore = max(all_scores) if all_scores else 1
        payload = {
            "tableInstance": {
                "id": reconciled_json['table']['id'],
                "idDataset": reconciled_json['table']['idDataset'],
                "name": reconciled_json['table']['name'],
                "nCols": reconciled_json["table"]["nCols"],
                "nRows": reconciled_json["table"]["nRows"],
                "nCells": reconciled_json["table"]["nCells"],
                "nCellsReconciliated": nCellsReconciliated,
                "lastModifiedDate": reconciled_json["table"]["lastModifiedDate"],
                "minMetaScore": minMetaScore,
                "maxMetaScore": maxMetaScore
            },
            "columns": {
                "byId": reconciled_json['columns'],
                "allIds": list(reconciled_json['columns'].keys())
            },
            "rows": {
                "byId": reconciled_json['rows'],
                "allIds": list(reconciled_json['rows'].keys())
            }
        }
        return payload

    def prepare_input_data_meteo(self, table, reconciliated_column_name, id_extender, properties, date_column_name, decimal_format):
        dates = {row_id: [row['cells'][date_column_name]['label'], [], date_column_name] for row_id, row in table['rows'].items()} if date_column_name else {}
        items = {reconciliated_column_name: {row_id: row['cells'][reconciliated_column_name]['metadata'][0]['id'] for row_id, row in table['rows'].items()}}
        weather_params = properties if date_column_name else []
        decimal_format = [decimal_format] if decimal_format else []

        payload = {
            "serviceId": id_extender,
            "dates": dates,
            "decimalFormat": decimal_format,
            "items": items,
            "weatherParams": weather_params
        }
        return payload

    def prepare_input_data_reconciledColumnExt(self, table, reconciliated_column_name, properties, id_extender):
        column_data = {
            row_id: [
                row['cells'][reconciliated_column_name]['label'],
                row['cells'][reconciliated_column_name].get('metadata', []),
                reconciliated_column_name
            ] for row_id, row in table['rows'].items()
        }
        items = {
            reconciliated_column_name: {
                row_id: row['cells'][reconciliated_column_name]['metadata'][0]['id']
                for row_id, row in table['rows'].items()
                if 'metadata' in row['cells'][reconciliated_column_name] and row['cells'][reconciliated_column_name]['metadata']
            }
        }
    
        payload = {
            "serviceId": id_extender,
            "column": column_data,
            "property": properties,
            "items": items
        }
        return payload
    
    def prepare_input_data_reconciled(self, table, reconciliated_column_name, properties, id_extender):
        column_data = {
            row_id: [
                row['cells'][reconciliated_column_name]['label'],
                row['cells'][reconciliated_column_name].get('metadata', []),
                reconciliated_column_name
            ] for row_id, row in table['rows'].items()
        }
        items = {
            reconciliated_column_name: {
                row_id: row['cells'][reconciliated_column_name]['metadata'][0]['id']
                for row_id, row in table['rows'].items()
                if 'metadata' in row['cells'][reconciliated_column_name] and row['cells'][reconciliated_column_name]['metadata']
            }
        }
    
        payload = {
            "serviceId": id_extender,
            "column": column_data,
            "property": properties,
            "items": items
        }
        return payload

    def send_extension_request(self, payload):
        try:
            print("Sending payload to extender service:")
            print(json.dumps(payload, indent=2))
            response = requests.post(self.api_url, headers=self.headers, json=payload)
            response.raise_for_status()
            print("Received response from extender service:")
            print(f"Status Code: {response.status_code}")
            print(f"Response Content: {response.text}")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            if response is not None:
                print(f"Response Content: {response.text}")
            raise
        except Exception as err:
            print(f"An error occurred: {err}")
            raise

    def compose_extension_table(self, table, extension_response):
        for column_name, column_data in extension_response['columns'].items():
            table['columns'][column_name] = {
                'id': column_name,
                'label': column_data['label'],
                'status': 'extended',
                'context': {},
                'metadata': [],
                'kind': 'extended',
                'annotationMeta': {}
            }
            for row_id, cell_data in column_data['cells'].items():
                table['rows'][row_id]['cells'][column_name] = {
                    'id': f"{row_id}${column_name}",
                    'label': cell_data['label'],
                    'metadata': cell_data['metadata']
                }
        return table

    def extend_column(self, table, column_name, extender_id, properties, other_params=None):
        """
        Standardized method to extend a column.
        
        :param table: The input table
        :param column_name: The name of the column to extend
        :param extender_id: The ID of the extender to use
        :param properties: The properties to extend
        :param other_params: A dictionary of additional parameters (optional)
        """
        other_params = other_params or {}
        
        input_data = self.prepare_input_data(table, column_name, extender_id, properties, other_params)
        extension_response = self.send_extension_request(input_data)
        extended_table = self.compose_extension_table(table, extension_response)
        backend_payload = self.create_backend_payload(extended_table)
        return extended_table, backend_payload

    def prepare_input_data(self, table, column_name, extender_id, properties, other_params):
        if extender_id == 'reconciledColumnExt':
            return self.prepare_input_data_reconciled(table, column_name, properties, extender_id)
        elif extender_id == 'meteoPropertiesOpenMeteo':
            date_column_name = other_params.get('date_column_name')
            decimal_format = other_params.get('decimal_format')
            if not date_column_name or not decimal_format:
                raise ValueError("date_column_name and decimal_format are required for meteoPropertiesOpenMeteo extender")
            return self.prepare_input_data_meteo(table, column_name, extender_id, properties, date_column_name, decimal_format)
        else:
            raise ValueError(f"Unsupported extender: {extender_id}")

    def get_extender(self, extender_id, response):
            """
            Given the extender's ID, returns the main information in JSON format
    
            :extender_id: the ID of the extender in question
            :response: JSON containing information about the extenders
            :return: JSON containing the main information of the extender
            """
            for extender in response:
                if extender['id'] == extender_id:
                    return {
                        'name': extender['name'],
                        'relativeUrl': extender['relativeUrl']
                    }
            return None
        
    def get_extender_data(self, debug: bool = False):
        """
        Fetches the raw data from the extenders endpoint.
        
        Args:
            debug (bool): If True, prints detailed information like response status, headers, etc.
            
        Returns:
            list or None: The raw response content if successful, else None.
        """
        try:
            # Example API call to the extenders endpoint
            response = requests.get(f"{self.base_url}/extenders", headers={"Authorization": f"Bearer {self.token}"})
            
            # Print response details if debug is enabled
            if debug:
                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {response.headers}")
                print(f"Response content: {response.content}")

            # Check for a successful response
            if response.status_code == 200:
                return response.json()  # Assuming the response content is in JSON format
            else:
                if debug:
                    print(f"Failed to retrieve extenders. Status Code: {response.status_code}")
        except Exception as e:
            if debug:
                print(f"Error occurred while fetching extenders: {e}")
        return None

    def clean_service_list(self, service_list):
        """
        Cleans and formats the service list.

        Args:
            service_list (list): Data regarding available services

        Returns:
            DataFrame: A DataFrame containing extenders' information.
        """
        # Define the DataFrame columns
        extenders_df = pd.DataFrame(columns=["id", "relativeUrl", "name"])

        # Iterate through each service and extract required details
        for service in service_list:
            extenders_df.loc[len(extenders_df)] = [
                service.get("id", ""),  # Use .get() to handle missing keys gracefully
                service.get("relativeUrl", ""),
                service.get("name", "")
            ]
        return extenders_df

    def get_extenders_list(self, debug: bool = False):
        """
        Provides a list of available extenders with their main information.

        Args:
            debug (bool): If True, prints response details like status code and headers.
            
        Returns:
            DataFrame or None: A DataFrame containing extenders' information or None if no data is found.
        """
        # Fetch the raw extender data
        extender_data = self.get_extender_data(debug=debug)

        # Check if the data is available
        if extender_data:
            # Clean and format the retrieved data into a structured DataFrame
            extenders_df = self.clean_service_list(extender_data)

            if debug:
                print("\nExtenders retrieved successfully!")
                print("\nStructured Extenders DataFrame:")
                print(extenders_df.to_string(index=False))  # Print the DataFrame without index for a cleaner view
            else:
                # Print a neatly formatted output when debug is False
                print("Extenders List:")
                print(extenders_df.to_string(index=False))  # Print the DataFrame without index

            return extenders_df
        else:
            if debug:
                print("Failed to retrieve extenders.")
            return None
    
    def get_extender_parameters(self, extender_id, print_params=False):
        """
        Retrieves the parameters needed for a specific extender service.

        :param extender_id: The ID of the extender service.
        :param print_params: (optional) Whether to print the retrieved parameters or not.
        :return: A dictionary containing the parameter details, or None if the extender is not found.
        """
        extender_data = self.get_extender_data()
        if not extender_data:
            return None
        
        for extender in extender_data:
            if extender['id'] == extender_id:
                parameters = extender.get('formParams', [])
                mandatory_params = [
                    {
                        'name': param['id'],
                        'type': param['inputType'],
                        'mandatory': 'required' in param.get('rules', []),
                        'description': param.get('description', ''),
                        'label': param.get('label', ''),
                        'infoText': param.get('infoText', ''),
                        'options': param.get('options', [])
                    } for param in parameters if 'required' in param.get('rules', [])
                ]
                optional_params = [
                    {
                        'name': param['id'],
                        'type': param['inputType'],
                        'mandatory': 'required' in param.get('rules', []),
                        'description': param.get('description', ''),
                        'label': param.get('label', ''),
                        'infoText': param.get('infoText', ''),
                        'options': param.get('options', [])
                    } for param in parameters if 'required' not in param.get('rules', [])
                ]

                param_dict = {
                    'mandatory': mandatory_params,
                    'optional': optional_params
                }

                if print_params:
                    print(f"Parameters for extender '{extender_id}':")
                    print("Mandatory parameters:")
                    for param in param_dict['mandatory']:
                        print(f"- {param['name']} ({param['type']}): Mandatory")
                        print(f"  Description: {param['description']}")
                        print(f"  Label: {param['label']}")
                        print(f"  Info Text: {param['infoText']}")
                        print(f"  Options: {param['options']}")
                        print("")

                    print("Optional parameters:")
                    for param in param_dict['optional']:
                        print(f"- {param['name']} ({param['type']}): Optional")
                        print(f"  Description: {param['description']}")
                        print(f"  Label: {param['label']}")
                        print(f"  Info Text: {param['infoText']}")
                        print(f"  Options: {param['options']}")
                        print("")

                return param_dict

        print(f"Extender with ID '{extender_id}' not found.")
        return None
    
    def get_parameter_options(self, extender_id, parameter_name):
        """
        Retrieves the options for a specified parameter of an extender service.

        :param extender_id: the ID of the extender service
        :param parameter_name: the name of the parameter to retrieve options for
        :return: a list of option IDs if found, None otherwise
        """
        extender_params = self.get_extender_parameters(extender_id)
        if not extender_params:
            return None

        for param_type in ['mandatory', 'optional']:
            for param in extender_params[param_type]:
                if param['name'] == parameter_name:
                    options = param.get('options', [])
                    if options:
                        return [option['id'] for option in options]

        return None
    
    def get_extender_details(self, extender_id, print_details=False):
        """
        Retrieves the parameters and options for a given extender service.

        Args:
            extender_id (str): The ID of the extender service.
            print_details (bool): Whether to print the details of the parameters and options.

        Returns:
            dict: A dictionary containing both parameters and options, or None if the extender is not found.
        """
        extender_data = self.get_extender_data()

        if not extender_data:
            return None

        # Search for the requested extender by ID
        for extender in extender_data:
            if extender['id'] == extender_id:
                # Retrieve parameter details and segregate into mandatory and optional
                parameters = extender.get('formParams', [])
                param_details = {
                    param['id']: {
                        'type': param['inputType'],
                        'mandatory': 'required' in param.get('rules', []),
                        'description': param.get('description', ''),
                        'label': param.get('label', ''),
                        'infoText': param.get('infoText', ''),
                        'options': param.get('options', [])
                    } for param in parameters
                }

                # Separate into mandatory and optional for better display
                mandatory_params = {k: v for k, v in param_details.items() if v['mandatory']}
                optional_params = {k: v for k, v in param_details.items() if not v['mandatory']}

                # Collect all options for easier access
                all_options = {param_name: [opt['id'] for opt in details['options']] 
                               for param_name, details in param_details.items() if details['options']}

                if print_details:
                    # Print the details for the parameters
                    print(f"Parameters for extender '{extender_id}':\n")
                    print("Mandatory parameters:")
                    for param_name, param_info in mandatory_params.items():
                        print(f"- {param_name} ({param_info['type']}): Mandatory")
                        print(f"  Description: {param_info['description']}")
                        print(f"  Label: {param_info['label']}")
                        print(f"  Info Text: {param_info['infoText']}")
                        print(f"  Options: {param_info['options']}")
                        print("")

                    print("Optional parameters:")
                    for param_name, param_info in optional_params.items():
                        print(f"- {param_name} ({param_info['type']}): Optional")
                        print(f"  Description: {param_info['description']}")
                        print(f"  Label: {param_info['label']}")
                        print(f"  Info Text: {param_info['infoText']}")
                        print(f"  Options: {param_info['options']}")
                        print("")

                    # Print options separately
                    if all_options:
                        print(f"Options for '{extender_id}':")
                        for param, options in all_options.items():
                            print(f"  - {param}: {options}")

                return {
                    'parameters': {
                        'mandatory': mandatory_params,
                        'optional': optional_params
                    },
                    'options': all_options
                }

        print(f"Extender with ID '{extender_id}' not found.")
        return None
        """
        Display options for all parameters of the given extenders.

        :param extender_ids: List of extender IDs to process
        :param debug: (optional) Whether to enable debug mode
        """
        for extender_id in extender_ids:
            print(f"\nRetrieving information for extender: {extender_id}")
            self.get_extender_info(extender_id, debug=debug)
            print("-" * 50)