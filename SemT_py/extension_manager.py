import requests
import json
import copy
import pandas as pd
from urllib.parse import urljoin
from copy import deepcopy
from IPython.display import display, HTML
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

    def send_extension_request(self, payload, debug=False):
        try:
            if debug:
                print("Sending payload to extender service:")
                print(json.dumps(payload, indent=2))
            response = requests.post(self.api_url, headers=self.headers, json=payload)
            response.raise_for_status()
            if debug:
                print("Received response from extender service:")
                print(f"Status Code: {response.status_code}")
                print(f"Response Content: {response.text}")
            return response.json()
        except requests.exceptions.HTTPError as http_err:
            if debug:
                print(f"HTTP error occurred: {http_err}")
                if response is not None:
                    print(f"Response Content: {response.text}")
            raise
        except Exception as err:
            if debug:
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

    def extend_column(self, table, column_name, extender_id, properties, other_params=None, debug=False):
        """
        Standardized method to extend a column.

        :param table: The input table
        :param column_name: The name of the column to extend
        :param extender_id: The ID of the extender to use
        :param properties: The properties to extend
        :param other_params: A dictionary of additional parameters (optional)
        :param debug: Boolean flag to enable/disable debug information
        """
        other_params = other_params or {}

        input_data = self.prepare_input_data(table, column_name, extender_id, properties, other_params)
        extension_response = self.send_extension_request(input_data, debug)
        extended_table = self.compose_extension_table(table, extension_response)
        backend_payload = self.create_backend_payload(extended_table)
        if debug:
            print("Extended table:", json.dumps(extended_table, indent=2))
            print("Backend payload:", json.dumps(backend_payload, indent=2))
        else:
            print("Column extended successfully!")
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
        
    def get_extender_data(self, debug=False):
        """
        Retrieves extender data from the backend with optional debug output.

        :param debug: If True, print detailed debug information.
        :return: JSON data from the API if successful, None otherwise.
        """
        try:
            # Correctly construct the URL
            url = urljoin(self.api_url, 'extenders/list')
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Debugging output
            if debug:
                print(f"Response status code: {response.status_code}")
                print(f"Response headers: {response.headers}")
                print(f"Response content: {response.text[:500]}...")  # Print first 500 characters for clarity
            
            # Check if the response is JSON
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                if debug:
                    print(f"Unexpected content type: {content_type}")
                    print("Full response content:")
                    print(response.text)
                return None

            return response.json()
        except requests.RequestException as e:
            if debug:
                print(f"Error occurred while retrieving extender data: {e}")
                if e.response is not None:
                    print(f"Response status code: {e.response.status_code}")
                    print(f"Response content: {e.response.text[:500]}...")  # Show first 500 characters of error content
            return None
        except json.JSONDecodeError as e:
            if debug:
                print(f"JSON decoding error: {e}")
                print(f"Raw response content: {response.text}")
            return None
    
    def clean_service_list(self, service_list):
        """
        Cleans and formats the service list into a DataFrame.

        :param service_list: Data regarding available services.
        :return: DataFrame containing extenders' information.
        """
        # Initialize a DataFrame with the specified columns
        reconciliators = pd.DataFrame(columns=["id", "relativeUrl", "name"])
        
        # Populate the DataFrame with the extenders' information
        for reconciliator in service_list:
            reconciliators.loc[len(reconciliators)] = [
                reconciliator["id"], reconciliator.get("relativeUrl", ""), reconciliator["name"]
            ]
        
        return reconciliators
    
    def get_extenders_list(self, debug=False):
        """
        Provides a list of available extenders with their main information.

        :param debug: If True, prints detailed debug information.
        :return: DataFrame containing extenders and their information.
        """
        response = self.get_extender_data(debug=debug)
        if response:
            df = self.clean_service_list(response)
            if debug:
                print("Retrieved Extenders List:")
                print(df)
            return df
        else:
            if debug:
                print("Failed to retrieve extenders data.")
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
    
    def get_extender_details(self, extender_id):
        """
        Retrieves the parameters and options for a given extender service.

        Args:
            extender_id (str): The ID of the extender service.

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

                # Format the results neatly for display
                formatted_result = {
                    'parameters': {
                        'mandatory': mandatory_params,
                        'optional': optional_params
                    },
                    'options': all_options
                }

                return formatted_result

        return None
    
    def visualize_extender_details(self, extender_id):
        details = self.get_extender_details(extender_id)
        if not details:
            print(f"Failed to retrieve details for extender '{extender_id}'.")
            return

        mandatory_params = details['parameters']['mandatory']
        optional_params = details['parameters']['optional']
        all_options = details['options']

        html_content = f"<h2>Details for extender '{extender_id}'</h2>"

        html_content += "<h3>Mandatory Parameters</h3>"
        if mandatory_params:
            html_content += "<ul>"
            for param_name, param_info in mandatory_params.items():
                html_content += f"<li><strong>{param_name}</strong> ({param_info['type']}): {param_info['description']}</li>"
            html_content += "</ul>"
        else:
            html_content += "<p>No mandatory parameters.</p>"

        html_content += "<h3>Optional Parameters</h3>"
        if optional_params:
            html_content += "<ul>"
            for param_name, param_info in optional_params.items():
                html_content += f"<li><strong>{param_name}</strong> ({param_info['type']}): {param_info['description']}</li>"
            html_content += "</ul>"
        else:
            html_content += "<p>No optional parameters.</p>"

        html_content += "<h3>Options</h3>"
        if all_options:
            html_content += "<ul>"
            for param, options in all_options.items():
                html_content += f"<li><strong>{param}</strong>: {', '.join(options)}</li>"
            html_content += "</ul>"
        else:
            html_content += "<p>No options available.</p>"

        display(HTML(html_content))