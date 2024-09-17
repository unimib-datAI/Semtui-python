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

    def prepare_input_data(self, table, reconciliated_column_name, id_extender, properties, date_column_name=None, decimal_format=None):
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

    def extend_column(self, table, reconciliated_column_name, id_extender, properties, date_column_name, decimal_format):
        input_data = self.prepare_input_data(table, reconciliated_column_name, id_extender, properties, date_column_name, decimal_format)
        extension_response = self.send_extension_request(input_data)
        extended_table = self.compose_extension_table(table, extension_response)
        backend_payload = self.create_backend_payload(extended_table)
        return extended_table, backend_payload

    def extend_reconciledColumnExt(self, table, reconciliated_column_name, id_extender, properties):
        input_data = self.prepare_input_data_reconciledColumnExt(table, reconciliated_column_name, properties, id_extender)
        extension_response = self.send_extension_request(input_data)
        extended_table = self.compose_extension_table(table, extension_response)
        backend_payload = self.create_backend_payload(extended_table)
        return extended_table, backend_payload

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
        
    def get_extender_data(self):
        """
        Retrieves extender data from the backend
        """
        try:
            # Correctly construct the URL
            url = urljoin(self.api_url, 'extenders/list')
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            
            # Debugging output
            print(f"Response status code: {response.status_code}")
            print(f"Response headers: {response.headers}")
            print(f"Response content: {response.text[:200]}...")  # Print first 200 characters
            
            # Check if the response is JSON
            content_type = response.headers.get('Content-Type', '')
            if 'application/json' not in content_type:
                print(f"Unexpected content type: {content_type}")
                print("Full response content:")
                print(response.text)
                return None
            
            return response.json()
        except requests.RequestException as e:
            print(f"Error occurred while retrieving extender data: {e}")
            if e.response is not None:
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text[:200]}...")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            print(f"Raw response content: {response.text}")
            return None
    
    def get_extenders_list(self):
        """
        Provides a list of available extenders with their main information.

        :return: DataFrame containing extenders and their information.
        """
        response = self.get_extender_data()
        if response:
            return self.clean_service_list(response)
        return None
    
    def clean_service_list(self, service_list):
        """
        Cleans and formats the service list.
        :param service_list: data regarding available services
        :return: DataFrame containing reconciliators information
        """
        reconciliators = pd.DataFrame(columns=["id", "relativeUrl", "name"])
        for reconciliator in service_list:
            reconciliators.loc[len(reconciliators)] = [
            reconciliator["id"], reconciliator["relativeUrl"], reconciliator["name"]]
        return reconciliators
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