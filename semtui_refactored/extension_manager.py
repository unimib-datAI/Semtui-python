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
        self.reconciliation_manager = ReconciliationManager(api_url, token)  # Dependency Injection

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

        :return: data of extension services in JSON format
        """
        try:
            response = requests.get(f"{self.api_url}extenders/list", headers=self.headers)
            response.raise_for_status()  # Raise an exception for HTTP errors
            return response.json()
        except requests.RequestException as e:
            print(f"Error occurred while retrieving extender data: {e}")
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

    def create_extension_payload(self, data, reconciliated_column_name, properties, extender_id, dates, weather_params, decimal_format=None):
        """
        Creates the payload for the extension request

        :param data: table in raw format
        :param reconciliated_column_name: the name of the column containing reconciled id
        :param properties: the properties to use in a list format
        :param extender_id: the ID of the extender service
        :param dates: a dictionary containing the date information for each row
        :param weather_params: a list of weather parameters to include in the request
        :param decimal_format: the decimal format to use for the values (default: None)
        :return: the request payload
        """
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
        """
        Function that, given the reconciliator's prefix, returns a dictionary 
        with all the service information

        :prefix_reconciliator: the prefix of the reconciliator in question
        :return: a dictionary with the reconciliator's information
        """
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
        """
        Specifying the column of interest returns the reconciliator's ID,
        if the column is reconciled

        :table: table in raw format
        :column_name: name of the column in question
        :return: the ID of the reconciliator used
        """
        prefix = list(table['columns'][column_name]['context'].keys())
        return self.get_reconciliator_from_prefix(prefix[0], reconciliator_response)['id']

    def check_entity(self, new_column_data):
        entity = False
        # Assuming newColumnData refers to a row payload, which contains a 'cells' list
        for cell in new_column_data['cells']:
            # Check if there is metadata and it's not empty
            if 'metadata' in cell and cell['metadata']:
                entity = True
                break
        return entity

    def parse_name_entities(self, entities, uri_reconciliator):
        """
        Function iterated in parseNameMetadata, works at the entity level

        :param entities: List of entities present in the cell/column
        :param uri_reconciliator: the URI of the affiliated knowledge graph
        :return: List of entities with updated names
        """
        for entity in entities:
            if 'id' in entity and ':' in entity['id']:
                entity_type = entity['id'].split(':')[1]  # Safely extract after colon
                entity['name'] = self.reconciliation_manager.parse_name_field(
                    entity.get('name', ''),  # Safely access 'name'
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

            # Retain existing metadata and add new metadata
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
        """
        Allows iterating the operations to insert a single column for
        all the properties to be inserted.

        :param table: table in raw format
        :param extension_data: data obtained from the extender
        :param properties: the properties to extend in the table
        :param reconciliator_response: response containing reconciliator information
        :return: the table with the new fields inserted
        """
        if 'columns' not in extension_data or 'meta' not in extension_data:
            raise ValueError("extensionData must contain 'columns' and 'meta'")

        # Iterating through new columns to be added
        for i, column_key in enumerate(extension_data['columns'].keys()):
            if i >= len(properties):
                raise IndexError("There are more columns to add than properties provided.")
            
            # Fetching reconciliator ID for the current column
            id_reconciliator = self.get_column_id_reconciliator(
                table, extension_data['meta'][column_key], reconciliator_response)
            
            # Adding the extended cell/column to the table
            table = self.add_extended_cell(
                table, extension_data['columns'][column_key], properties[i], id_reconciliator, reconciliator_response)
            
        return table

    def process_format_and_construct_payload(self, reconciled_json, extended_json, reconciliated_column_name, properties, extender_id):
        def merge_reconciled_and_extended(reconciled_json, extended_json, reconciliated_column_name, properties, extender_id):
            merged_json = reconciled_json.copy()
            merged_json['table'] = extended_json['table']
            
            if extender_id == "meteoPropertiesOpenMeteo":
                # Add new columns for weather properties with the prefix
                for prop in properties:
                    new_col_id = f"{reconciliated_column_name}_{prop}"
                    merged_json['columns'][new_col_id] = {
                        'id': new_col_id,
                        'label': new_col_id,
                        'metadata': [],
                        'status': 'empty',
                        'context': {}
                    }
                
                # Remove unwanted columns
                columns_to_remove = ['City_id', 'City_name', 'id_City', 'name_City']
                for col in columns_to_remove:
                    if col in merged_json['columns']:
                        del merged_json['columns'][col]
                
                # Ensure City column has the correct status
                merged_json['columns']['City']['status'] = 'pending'
                
            elif extender_id == "reconciledColumnExt":
                # Update City column
                merged_json['columns']['City'].update({
                    'status': 'reconciliated',
                    'metadata': [{
                        'id': 'None:',
                        'match': True,
                        'score': 0,
                        'name': {'value': '', 'uri': ''},
                        'entity': [
                            {
                                'id': 'wd:Q29934236',
                                'name': {
                                    'value': 'GlobeCoordinate',
                                    'uri': 'http://149.132.176.67:3002/map?polyline=Q29934236'
                                },
                                'score': 0,
                                'match': True,
                                'type': []
                            },
                            {
                                'id': 'georss:point',
                                'name': {
                                    'value': 'point',
                                    'uri': 'http://149.132.176.67:3002/map?polyline=point'
                                },
                                'score': 0,
                                'match': True,
                                'type': []
                            }
                        ]
                    }],
                    'annotationMeta': {
                        'annotated': True,
                        'match': {'value': True, 'reason': 'reconciliator'},
                        'lowestScore': 0,
                        'highestScore': 0
                    }
                })

                # Add new columns for reconciled properties
                for prop in properties:
                    new_col_id = f"{prop}_{reconciliated_column_name}"
                    merged_json['columns'][new_col_id] = {
                        'id': new_col_id,
                        'label': new_col_id,
                        'metadata': [],
                        'status': 'empty',
                        'context': {}
                    }

                # Remove unwanted columns
                columns_to_remove = ['City_id', 'City_name']
                for col in columns_to_remove:
                    if col in merged_json['columns']:
                        del merged_json['columns'][col]

            # Update rows
            for row_id, row in merged_json['rows'].items():
                if extender_id == "meteoPropertiesOpenMeteo":
                    for prop in properties:
                        new_cell_id = f"{reconciliated_column_name}_{prop}"
                        value = extended_json['rows'][row_id]['cells'][prop]['label']
                        if isinstance(value, list):
                            value = str(value[0]).replace('.', ',')
                        else:
                            value = str(value).replace('.', ',')
                        row['cells'][new_cell_id] = {
                            'id': f"{row_id}${new_cell_id}",
                            'label': value,
                            'metadata': []
                        }
                    
                    # Remove unwanted cells
                    cells_to_remove = ['City_id', 'City_name', 'id_City', 'name_City']
                    for cell in cells_to_remove:
                        if cell in row['cells']:
                            del row['cells'][cell]
                    
                elif extender_id == "reconciledColumnExt":
                    city_metadata = row['cells']['City']['metadata'][0]
                    
                    # Update City cell
                    row['cells']['City'].update({
                        'metadata': [{
                            'id': city_metadata['id'],
                            'name': {
                                'value': city_metadata['name']['value'],
                                'uri': f"http://149.132.176.67:3002/map?polyline={city_metadata['id'].split(':')[1]}"
                            },
                            'feature': [{'id': 'all_labels', 'value': 100}],
                            'score': city_metadata['score'],
                            'match': city_metadata['match'],
                            'type': city_metadata['type']
                        }],
                        'annotationMeta': {
                            'annotated': True,
                            'match': {'value': True, 'reason': 'reconciliator'},
                            'lowestScore': 1,
                            'highestScore': 1
                        }
                    })

                    # Add id_City and name_City cells
                    row['cells']['id_City'] = {
                        'id': f"{row_id}$id_City",
                        'label': city_metadata['id'].split(':')[1],
                        'metadata': []
                    }
                    row['cells']['name_City'] = {
                        'id': f"{row_id}$name_City",
                        'label': city_metadata['name']['value'],
                        'metadata': []
                    }

                    # Remove unwanted cells
                    cells_to_remove = ['City_id', 'City_name']
                    for cell in cells_to_remove:
                        if cell in row['cells']:
                            del row['cells'][cell]
            
            return merged_json

        merged_json = merge_reconciled_and_extended(reconciled_json, extended_json, reconciliated_column_name, properties, extender_id)
        
        # Calculate nCellsReconciliated
        nCellsReconciliated = sum(
            1 for row in merged_json['rows'].values() for cell in row['cells'].values() if cell.get('annotationMeta', {}).get('annotated', False)
        )
        
        # Construct the payload
        payload = {
            "tableInstance": {
                "id": reconciled_json['table']['id'],
                "idDataset": reconciled_json['table']['idDataset'],
                "name": reconciled_json['table']['name'],
                "nCols": merged_json["table"]["nCols"],
                "nRows": merged_json["table"]["nRows"],
                "nCells": merged_json["table"]["nCells"],
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
    
    def process_format_and_construct_payload_reconciled(self, reconciled_json, extended_json, reconciliated_column_name, properties):
        def merge_reconciled_and_extended(reconciled_json, extended_json, reconciliated_column_name, properties):
            merged_json = reconciled_json.copy()
            merged_json['table'] = extended_json['table']
            
            # Add new columns for reconciled properties
            for prop in properties:
                new_col_id = f"{prop}_{reconciliated_column_name}"
                merged_json['columns'][new_col_id] = {
                    'id': new_col_id,
                    'label': new_col_id,
                    'metadata': [],
                    'status': 'empty',
                    'context': {}
                }
            
            # Update rows with reconciled data
            for row_id, row in merged_json['rows'].items():
                for prop in properties:
                    new_cell_id = f"{prop}_{reconciliated_column_name}"
                    if new_cell_id in extended_json['rows'][row_id]['cells']:
                        row['cells'][new_cell_id] = extended_json['rows'][row_id]['cells'][new_cell_id]
                
                # Update the status of the reconciliated column
                if 'annotationMeta' in row['cells'][reconciliated_column_name]:
                    row['cells'][reconciliated_column_name]['annotationMeta']['match']['reason'] = 'reconciliator'
            
            # Update the status of the reconciliated column
            merged_json['columns'][reconciliated_column_name]['status'] = 'reconciliated'
            
            return merged_json

        merged_json = merge_reconciled_and_extended(reconciled_json, extended_json, reconciliated_column_name, properties)
        
        # Calculate nCellsReconciliated
        nCellsReconciliated = sum(
            1 for row in merged_json['rows'].values() for cell in row['cells'].values() if cell.get('annotationMeta', {}).get('annotated', False)
        )
        
        # Construct the payload
        payload = {
            "tableInstance": {
                "id": reconciled_json['table']['id'],
                "idDataset": reconciled_json['table']['idDataset'],
                "name": reconciled_json['table']['name'],
                "nCols": merged_json["table"]["nCols"],
                "nRows": merged_json["table"]["nRows"],
                "nCells": merged_json["table"]["nCells"],
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

    def extend_column(self, table, reconciliated_column_name, extender_id, properties, date_column_name=None, decimal_format=None):
        try:
            if extender_id == "meteoPropertiesOpenMeteo":
                extended_table = self.extend_other_properties(table, reconciliated_column_name, extender_id, properties, date_column_name, decimal_format)
            elif extender_id == "reconciledColumnExt":
                extended_table = self.extend_reconciled_column(table, reconciliated_column_name, properties)
            else:
                raise ValueError(f"Unsupported extender_id: {extender_id}")
            
            if extended_table is None:
                print("Failed to extend table.")
                return None, None
            
            extension_payload = self.process_format_and_construct_payload(
                reconciled_json=table,
                extended_json=extended_table,
                reconciliated_column_name=reconciliated_column_name,
                properties=properties,
                extender_id=extender_id
            )

            return extended_table, extension_payload
        except Exception as e:
            print(f"Error in extend_column: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None  
        
    def extend_reconciled_column(self, table, reconciliated_column_name, properties):
        extended_table = table.copy()
        
        # Add new columns for the properties to be extended
        for prop in properties:
            new_column_name = f"{reconciliated_column_name}_{prop}"
            if new_column_name not in extended_table['columns']:
                extended_table['columns'][new_column_name] = {
                    'id': new_column_name,
                    'label': new_column_name,
                    'status': 'empty',
                    'context': {},
                    'metadata': []
                }
        
        # Iterate over each row to extend the properties
        for row_key, row_data in extended_table['rows'].items():
            city_cell = row_data['cells'].get(reconciliated_column_name)
            if city_cell and city_cell.get('metadata'):
                metadata = city_cell['metadata'][0]  # The metadata we need is in the first item of the list
                
                for prop in properties:
                    new_column_name = f"{reconciliated_column_name}_{prop}"
                    value = None
                    
                    if prop == 'id':
                        value = metadata.get('id')
                    elif prop == 'name':
                        value = metadata.get('name', {}).get('value')
                    
                    if value:
                        row_data['cells'][new_column_name] = {
                            'id': f"{row_key}${new_column_name}",
                            'label': value,
                            'metadata': []
                        }
            else:
                print(f"No metadata found for row {row_key} in column '{reconciliated_column_name}'")
        
        return extended_table
    
    def extend_other_properties(self, table, reconciliated_column_name, extender_id, properties, date_column_name, decimal_format):
        reconciliator_response = self.reconciliation_manager.get_reconciliator_data()
        extender_data = self.get_extender(extender_id, self.get_extender_data())
        
        if extender_data is None:
            raise ValueError(f"Extender with ID '{extender_id}' not found.")
        
        url = self.api_url + "extenders/" + extender_data['relativeUrl']
        
        dates = {}
        for row_key, row_data in table['rows'].items():
            date_value = row_data['cells'].get(date_column_name, {}).get('label')
            if date_value:
                dates[row_key] = [date_value]
            else:
                print(f"Missing or invalid date for row {row_key}, skipping this row.")
                continue
        
        weather_params = properties if extender_id == "meteoPropertiesOpenMeteo" else None
        
        payload = self.create_extension_payload(table, reconciliated_column_name, properties, extender_id, dates, weather_params, decimal_format)
        
        headers = {"Accept": "application/json"}

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
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
    