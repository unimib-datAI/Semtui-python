import requests
import json
import pandas as pd
from .reconciliation_manager import ReconciliationManager  # Assuming this class is defined in reconciliation_manager.py
from .token_manager import TokenManager

class ExtensionManager:
    def __init__(self, api_url, token):
        self.api_url = api_url
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        self.reconciliation_manager = ReconciliationManager(api_url, token)

    def get_extender(self, extender_id, response):
        for extender in response:
            if extender['id'] == extender_id:
                return {
                    'name': extender['name'],
                    'relativeUrl': extender['relativeUrl']
                }
        return None
    
    def get_extender_data(self):
        try:
            response = requests.get(f"{self.api_url}extenders/list", headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            print(f"Error occurred while retrieving extender data: {e}")
            return None
    
    def get_extenders_list(self):
        response = self.get_extender_data()
        if response:
            return self.clean_service_list(response)
        return None
    
    def clean_service_list(self, service_list):
        reconciliators = pd.DataFrame(columns=["id", "relativeUrl", "name"])
        for reconciliator in service_list:
            reconciliators.loc[len(reconciliators)] = [
                reconciliator["id"], reconciliator["relativeUrl"], reconciliator["name"]
            ]
        return reconciliators

    def create_extension_payload(self, data, reconciliated_column_name, properties, extender_id, dates, weather_params, decimal_format=None):
        items = {}
        if 'rows' not in data:
            raise KeyError("The 'data' dictionary does not contain the 'rows'")
        rows = data['rows'].keys()
        for row in rows:
            if 'cells' not in data['rows'][row]:
                raise KeyError(f"The 'data['rows'][{row}]' dictionary does not contain the 'cells' key.")
            
            cell = data['rows'][row]['cells'].get(reconciliated_column_name)
            if cell and cell.get('annotationMeta', {}).get('match', {}).get('value') == True:
                for metadata in cell.get('metadata', []):
                    if metadata.get('match') == True:
                        items[row] = metadata.get('id')
                        break    
        payload = {
            "serviceId": extender_id,
            "items": {
                str(reconciliated_column_name): items
            },
            "property": properties,
            "dates": dates,
            "weatherParams": weather_params,
            "decimalFormat": decimal_format or []
        }
        
        return payload
    
    def get_reconciliator_from_prefix(self, prefix_reconciliator, response):
        for reconciliator in response:
            if reconciliator['prefix'] == prefix_reconciliator:
                return {
                    'uri': reconciliator['uri'],
                    'id': reconciliator['id'],
                    'name': reconciliator['name'],
                    'relativeUrl': reconciliator['relativeUrl']
                }
        return None

    def get_column_id_reconciliator(self, table, column_name, reconciliator_response):
        prefix = list(table['columns'][column_name]['context'].keys())
        return self.get_reconciliator_from_prefix(prefix[0], reconciliator_response)['id']

    def check_entity(self, new_column_data):
        entity = False
        for cell in new_column_data['cells']:
            if 'metadata' in cell and cell['metadata']:
                entity = True
                break
        return entity

    def parse_name_entities(self, entities, uri_reconciliator):
        for entity in entities:
            if 'id' in entity and ':' in entity['id']:
                entity_type = entity['id'].split(':')[1]
                entity['name'] = self.reconciliation_manager.parse_name_field(
                    entity.get('name', ''),
                    uri_reconciliator,
                    entity_type
                )
        return entities

    def add_extended_cell(self, table, new_column_data, new_column_name, id_reconciliator, reconciliator_response):
        if 'cells' not in new_column_data:
            raise ValueError("newColumnData must contain 'cells'")
        
        row_keys = new_column_data['cells']
        entity = self.check_entity(new_column_data)
        column_type = new_column_data.get('kind', 'entity' if entity else 'literal')

        for row_key in row_keys:
            cell_data = new_column_data['cells'][row_key]
            new_cell = table['rows'][row_key]['cells'].setdefault(new_column_name, {})
            new_cell['id'] = f"{row_key}${new_column_name}"
            new_cell['label'] = cell_data.get('label', '')

            existing_metadata = table['rows'][row_key]['cells'].get(new_column_name, {}).get('metadata', [])
            new_metadata = cell_data.get('metadata', [])
            new_cell['metadata'] = existing_metadata + new_metadata

            uri_reconciliator = self.reconciliation_manager.get_reconciliator(id_reconciliator, reconciliator_response)['uri']
            new_cell['metadata'] = self.parse_name_entities(new_cell['metadata'], uri_reconciliator)

            if column_type == 'entity':
                new_cell['annotationMeta'] = self.reconciliation_manager.create_annotation_meta_cell(new_cell['metadata'])
            else:
                new_cell['annotationMeta'] = {}

        return table
    
    def add_extended_columns(self, table, extension_data, properties, reconciliator_response):
        if 'columns' not in extension_data or 'meta' not in extension_data:
            raise ValueError("extensionData must contain 'columns' and 'meta'")

        for i, column_key in enumerate(extension_data['columns'].keys()):
            if i >= len(properties):
                raise IndexError("There are more columns to add than properties provided.")
            
            id_reconciliator = self.get_column_id_reconciliator(
                table, extension_data['meta'][column_key], reconciliator_response)
            
            table = self.add_extended_cell(
                table, extension_data['columns'][column_key], properties[i], id_reconciliator, reconciliator_response)
            
        return table

    def extend_column(self, table, reconciliated_column_name, id_extender, properties, date_column_name=None, decimal_format=None):
        try:
            if id_extender == "reconciledColumnExt":
                extended_table = self.extend_reconciled_column(table, reconciliated_column_name, properties)
            elif id_extender == "meteoPropertiesOpenMeteo":
                extended_table = self.extend_meteo_properties(table, reconciliated_column_name, properties, date_column_name, decimal_format)
            else:
                raise ValueError(f"Unsupported extender_id: {id_extender}")

            if extended_table is None:
                print("Failed to extend table.")
                return None, None

            extension_payload = self.process_format_and_construct_payload(
                reconciled_json=table,
                extended_json=extended_table,
                reconciliated_column_name=reconciliated_column_name,
                properties=properties
            )

            return extended_table, extension_payload
        except Exception as e:
            print(f"Error in extend_column: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None

    def extend_reconciled_column(self, table, reconciliated_column_name, properties):
        extended_table = table.copy()

        for prop in properties:
            new_column_name = f"{prop}_{reconciliated_column_name}"
            extended_table['columns'][new_column_name] = {
                'id': new_column_name,
                'label': new_column_name,
                'status': 'empty',
                'context': {},
                'metadata': []
            }
            for row_key, row_data in extended_table['rows'].items():
                cell = row_data['cells'].get(reconciliated_column_name)
                if cell and cell.get('annotationMeta', {}).get('match', {}).get('value') == True:
                    for metadata in cell.get('metadata', []):
                        if metadata.get('match') == True:
                            value = metadata.get(prop)
                            if value:
                                row_data['cells'][new_column_name] = {
                                    'id': f"{row_key}${new_column_name}",
                                    'label': value,
                                    'metadata': []
                                }
                            break
        return extended_table

    def extend_meteo_properties(self, table, reconciliated_column_name, properties, date_column_name, decimal_format):
        reconciliator_response = self.reconciliation_manager.get_reconciliator_data()
        extender_data = self.get_extender("meteoPropertiesOpenMeteo", self.get_extender_data())

        if extender_data is None:
            raise ValueError(f"Extender with ID 'meteoPropertiesOpenMeteo' not found.")

        url = self.api_url + "extenders/" + extender_data['relativeUrl']

        dates = {}
        for row_key, row_data in table['rows'].items():
            date_value = row_data['cells'].get(date_column_name, {}).get('label')
            if date_value:
                dates[row_key] = [date_value]
            else:
                print(f"Missing or invalid date for row {row_key}, skipping this row.")
                continue

        payload = self.create_extension_payload(table, reconciliated_column_name, properties, "meteoPropertiesOpenMeteo", dates, properties, decimal_format)

        headers = {"Accept": "application/json"}

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            data = self.parse_decimal_format(data, decimal_format)
            extended_table = self.add_extended_columns(table, data, properties, reconciliator_response)
            return extended_table
        except requests.RequestException as e:
            print(f"An error occurred while making the request: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            return None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None

    def parse_decimal_format(self, data, decimal_format):
        if decimal_format == "comma":
            for row_key, row_data in data.get('rows', {}).items():
                for cell_key, cell_data in row_data.get('cells', {}).items():
                    if 'label' in cell_data and isinstance(cell_data['label'], str):
                        cell_data['label'] = cell_data['label'].replace('.', ',')
        return data
    
    def process_format_and_construct_payload(self, reconciled_json, extended_json, reconciliated_column_name, properties):
        def merge_reconciled_and_extended(reconciled_json, extended_json, reconciliated_column_name, properties):
            merged_json = reconciled_json.copy()
            merged_json['table'] = extended_json['table']

            for prop in properties:
                new_col_id = f"{reconciliated_column_name}_{prop}"
                merged_json['columns'][new_col_id] = {
                    'id': new_col_id,
                    'label': new_col_id,
                    'metadata': [],
                    'status': 'empty',
                    'context': {}
                }

            for row_id, row in merged_json['rows'].items():
                for prop in properties:
                    new_cell_id = f"{reconciliated_column_name}_{prop}"
                    row['cells'][new_cell_id] = {
                        'id': f"{row_id}${new_cell_id}",
                        'label': extended_json['rows'][row_id]['cells'][prop]['label'],
                        'metadata': []
                    }
                    if prop in row['cells']:
                        del row['cells'][prop]
            return merged_json

        merged_json = merge_reconciled_and_extended(reconciled_json, extended_json, reconciliated_column_name, properties)

        nCellsReconciliated = sum(
            1 for row in merged_json['rows'].values() for cell in row['cells'].values() if cell.get('annotationMeta', {}).get('annotated', False)
        )

        payload = {
            "tableInstance": {
                "id": reconciled_json['table']['id'],
                "idDataset": reconciled_json['table']['idDataset'],
                "name": reconciled_json['table']['name'],
                "nCols": merged_json["table"]["nCols"],
                "nRows": merged_json["nRows"],
                "nCells": merged_json["nCells"],
                "nCellsReconciliated": nCellsReconciliated,
                "lastModifiedDate": merged_json["table"]["lastModifiedDate"],
                "minMetaScore": merged_json["table"].get("minMetaScore", 0),
                "maxMetaScore": merged_json["table"].get("maxMetaScore", 1)
            },
            "columns": {
                "byId": merged_json['columns'],
                "allIds": list(merged_json['columns'].keys())
            },
            "rows": {
                "byId": merged_json['rows'],
                "allIds": list(merged_json['rows'].keys())
            }
        }

        return payload

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
    