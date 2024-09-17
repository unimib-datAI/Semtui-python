import requests
import json
import copy
import pandas as pd 
import datetime
from urllib.parse import urljoin
from .token_manager import TokenManager

class ReconciliationManager:
    def __init__(self, base_url, token_manager):
        self.base_url = base_url.rstrip('/') + '/'
        self.api_url = urljoin(self.base_url, 'api/')
        self.token_manager = token_manager

    def _get_headers(self):
        return {
            'Authorization': f'Bearer {self.token_manager.get_token()}',
            'Content-Type': 'application/json;charset=UTF-8',
            'Accept': 'application/json, text/plain, */*'
        }

    def prepare_input_data(self, original_input, column_name, reconciliator_id, optional_columns):
        input_data = {
            "serviceId": reconciliator_id,
            "items": [{"id": column_name, "label": column_name}],
            "secondPart": {},
            "thirdPart": {}
        }

        for row_id, row_data in original_input['rows'].items():
            main_column_value = row_data['cells'][column_name]['label']
            input_data['items'].append({"id": f"{row_id}${column_name}", "label": main_column_value})

            if reconciliator_id in ['geocodingHere', 'geocodingGeonames']:
                second_part_value = row_data['cells'].get(optional_columns[0], {}).get('label', '')
                third_part_value = row_data['cells'].get(optional_columns[1], {}).get('label', '')
                input_data['secondPart'][row_id] = [second_part_value, [], optional_columns[0]]
                input_data['thirdPart'][row_id] = [third_part_value, [], optional_columns[1]]

        return input_data

    def send_reconciliation_request(self, input_data, reconciliator_id):
        url = urljoin(self.api_url, f'reconciliators/{reconciliator_id}')
        headers = self._get_headers()
        
        try:
            response = requests.post(url, json=input_data, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error: {e}")
            return None

    
    def compose_reconciled_table(self, original_input, reconciliation_output, column_name):
        final_payload = copy.deepcopy(original_input)

        # Update only the necessary fields
        final_payload['table']['lastModifiedDate'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        
        # Update the reconciled column information
        final_payload['columns'][column_name]['status'] = 'reconciliated'
        final_payload['columns'][column_name]['context'] = {
            'georss': {
                'uri': 'http://www.google.com/maps/place/',
                'total': len(reconciliation_output) - 1,
                'reconciliated': len(reconciliation_output) - 1
            }
        }
        final_payload['columns'][column_name]['kind'] = 'entity'
        final_payload['columns'][column_name]['annotationMeta'] = {
            'annotated': True,
            'match': {'value': True},
            'lowestScore': 1,
            'highestScore': 1
        }

        column_metadata = next(item for item in reconciliation_output if item['id'] == column_name)
        final_payload['columns'][column_name]['metadata'] = column_metadata['metadata']

        # Update cell information for the reconciled column
        nCellsReconciliated = 0
        for item in reconciliation_output:
            if item['id'] != column_name:
                row_id, cell_id = item['id'].split('$')
                cell = final_payload['rows'][row_id]['cells'][cell_id]

                metadata = item['metadata'][0]
                cell['metadata'] = [metadata]

                cell['annotationMeta'] = {
                    'annotated': True,
                    'match': {'value': metadata['match']},
                    'lowestScore': metadata['score'],
                    'highestScore': metadata['score']
                }
                nCellsReconciliated += 1

        # Update nCellsReconciliated in the table information
        final_payload['table']['nCellsReconciliated'] = nCellsReconciliated

        return final_payload

    def restructure_payload(self, payload):
        base_uri = "http://149.132.176.67:3002/map?polyline="
    
        # Get list of columns that are reconciliated
        reconciliated_columns = [col_key for col_key, col in payload['columns'].items() if col.get('status') == 'reconciliated']

        # Restructure columns
        for column_key in reconciliated_columns:
            column = payload['columns'][column_key]
    
            # Build new metadata
            new_metadata = [{
                'id': 'None:',
                'match': True,
                'score': 0,
                'name': {'value': '', 'uri': ''},
                'entity': []
            }]
    
            for item in column.get('metadata', []):
                new_entity = {
                    'id': item['id'],
                    'name': {
                        'value': item['name'],
                        'uri': f"{base_uri}{item['id'].split(':')[-1]}"
                    },
                    'score': 0,  # Column metadata score is set to 0
                    'match': True,
                    'type': [{'id': t['id'], 'name': t['name']} for t in item.get('type', [])]
                }
                new_metadata[0]['entity'].append(new_entity)
    
            column['metadata'] = new_metadata

            # Compute lowest and highest scores from cell metadata
            scores = []
            for row in payload['rows'].values():
                cell = row['cells'].get(column_key)
                if cell and 'metadata' in cell and len(cell['metadata']) > 0:
                    score = cell['metadata'][0].get('score', 0)
                    scores.append(score)
    
            column['annotationMeta'] = {
                'annotated': True,
                'match': {'value': True, 'reason': 'reconciliator'},
                'lowestScore': min(scores) if scores else 0,
                'highestScore': max(scores) if scores else 0
            }
    
            if 'kind' in column:
                del column['kind']
        
        # Restructure rows
        for row in payload['rows'].values():
            for cell_key, cell in row['cells'].items():
                if cell_key in reconciliated_columns:
                    if 'metadata' in cell:
                        for idx, item in enumerate(cell['metadata']):
                            # Build item with fields in desired order
                            new_item = {}
                            new_item['id'] = item['id']
                            new_item['name'] = {
                                'value': item['name'],
                                'uri': f"{base_uri}{item['id'].split(':')[-1]}"
                            }
                            if 'feature' in item:
                                new_item['feature'] = item['feature']
                            new_item['score'] = item.get('score', 0)
                            new_item['match'] = True  # Set match to True
                            new_item['type'] = [{'id': t['id'], 'name': t['name']} for t in item.get('type', [])]
    
                            # Replace the item in cell['metadata'] with the new dictionary
                            cell['metadata'][idx] = new_item
    
                    if 'annotationMeta' in cell:
                        cell['annotationMeta']['match'] = {'value': True, 'reason': 'reconciliator'}
                        # Set lowestScore and highestScore from cell's score
                        if 'metadata' in cell and len(cell['metadata']) > 0:
                            score = cell['metadata'][0].get('score', 0)
                            cell['annotationMeta']['lowestScore'] = score
                            cell['annotationMeta']['highestScore'] = score
    
        return payload
    
    def create_backend_payload(self, final_payload):
        nCellsReconciliated = sum(
            1 for row in final_payload['rows'].values()
            for cell in row['cells'].values()
            if cell.get('annotationMeta', {}).get('annotated', False)
        )
        all_scores = [
            cell.get('annotationMeta', {}).get('lowestScore', float('inf'))
            for row in final_payload['rows'].values()
            for cell in row['cells'].values()
            if cell.get('annotationMeta', {}).get('annotated', False)
        ]
        minMetaScore = min(all_scores) if all_scores else 0
        maxMetaScore = max(all_scores) if all_scores else 1
    
        table_data = final_payload['table']
        columns = final_payload.get('columns', {})
        rows = final_payload.get('rows', {})
    
        backend_payload = {
            "tableInstance": {
                "id": table_data.get("id"),
                "idDataset": table_data.get("idDataset"),
                "name": table_data.get("name"),
                "nCols": table_data.get("nCols", 0),
                "nRows": table_data.get("nRows", 0),
                "nCells": table_data.get("nCells", 0),
                "nCellsReconciliated": nCellsReconciliated,
                "lastModifiedDate": table_data.get("lastModifiedDate", ""),
                "minMetaScore": minMetaScore,
                "maxMetaScore": maxMetaScore
            },
            "columns": {
                "byId": columns,
                "allIds": list(columns.keys())
            },
            "rows": {
                "byId": rows,
                "allIds": list(rows.keys())
            }
        }
    
        return backend_payload


    def reconcile(self, table_data, column_name, reconciliator_id, optional_columns):
        if reconciliator_id not in ['geocodingHere', 'geocodingGeonames', 'geonames']:
            raise ValueError("Invalid reconciliator ID. Please use 'geocodingHere', 'geocodingGeonames', or 'geonames'.")
    
        input_data = self.prepare_input_data(table_data, column_name, reconciliator_id, optional_columns)
        response_data = self.send_reconciliation_request(input_data, reconciliator_id)
    
        if response_data:
            # Compose the reconciled table
            final_payload = self.compose_reconciled_table(table_data, response_data, column_name)
            # Restructure the final_payload to the desired format
            final_payload = self.restructure_payload(final_payload)
            # Create the backend payload using the restructured final_payload
            backend_payload = self.create_backend_payload(final_payload)
            return final_payload, backend_payload
        else:
            return None, None

    def get_reconciliator_data(self):
        try:
            url = urljoin(self.api_url, 'reconciliators/list')
            headers = self._get_headers()
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            
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
            print(f"Request error occurred while retrieving reconciliator data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"Response status code: {e.response.status_code}")
                print(f"Response content: {e.response.text[:200]}...")
            return None
        except json.JSONDecodeError as e:
            print(f"JSON decoding error: {e}")
            print(f"Raw response content: {response.text}")
            return None

    def get_reconciliators_list(self):
        response = self.get_reconciliator_data()
        if response is not None:
            try:
                return self.clean_service_list(response)
            except Exception as e:
                print(f"Error in clean_service_list: {e}")
                return pd.DataFrame()  # Return an empty DataFrame instead of None
        return pd.DataFrame()  # Return an empty DataFrame if response is None

    def clean_service_list(self, service_list):
        if not isinstance(service_list, list):
            print(f"Expected a list, but got {type(service_list)}: {service_list}")
            return pd.DataFrame()

        reconciliators = []
        for reconciliator in service_list:
            if isinstance(reconciliator, dict) and all(key in reconciliator for key in ["id", "relativeUrl", "name"]):
                reconciliators.append({
                    "id": reconciliator["id"],
                    "relativeUrl": reconciliator["relativeUrl"],
                    "name": reconciliator["name"]
                })
            else:
                print(f"Skipping invalid reconciliator data: {reconciliator}")
        
        return pd.DataFrame(reconciliators)
    
    def get_reconciliator_parameters(self, id_reconciliator, print_params=False):
        """
        Retrieves the parameters needed for a specific reconciliator service.

        :param id_reconciliator: the ID of the reconciliator service
        :param print_params: (optional) whether to print the retrieved parameters or not
        :return: a dictionary containing the parameter details
        """
        mandatory_params = [
            {'name': 'table', 'type': 'json', 'mandatory': True, 'description': 'The table data in JSON format'},
            {'name': 'columnName', 'type': 'string', 'mandatory': True, 'description': 'The name of the column to reconcile'},
            {'name': 'idReconciliator', 'type': 'string', 'mandatory': True, 'description': 'The ID of the reconciliator to use'}
        ]
        
        reconciliator_data = self.get_reconciliator_data()
        if not reconciliator_data:
            return None

        for reconciliator in reconciliator_data:
            if reconciliator['id'] == id_reconciliator:
                parameters = reconciliator.get('formParams', [])
                optional_params = [
                    {
                        'name': param['id'],
                        'type': param['inputType'],
                        'mandatory': 'required' in param.get('rules', []),
                        'description': param.get('description', ''),
                        'label': param.get('label', ''),
                        'infoText': param.get('infoText', '')
                    } for param in parameters
                ]

                param_dict = {
                    'mandatory': mandatory_params,
                    'optional': optional_params
                }

                if print_params:
                    print(f"Parameters for reconciliator '{id_reconciliator}':")
                    print("Mandatory parameters:")
                    for param in param_dict['mandatory']:
                        print(f"- {param['name']} ({param['type']}): Mandatory")
                        print(f"  Description: {param['description']}")
                    
                    print("\nOptional parameters:")
                    for param in param_dict['optional']:
                        mandatory = "Mandatory" if param['mandatory'] else "Optional"
                        print(f"- {param['name']} ({param['type']}): {mandatory}")
                        print(f"  Description: {param['description']}")
                        print(f"  Label: {param['label']}")
                        print(f"  Info Text: {param['infoText']}")

                return param_dict

        return None