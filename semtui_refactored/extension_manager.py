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
        self.api_url = urljoin(self.base_url, 'api/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }
        
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
        }
        
        # Add decimal_format to the payload only if it's specified
        if decimal_format:
            payload["decimalFormat"] = decimal_format

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

    def parse_name_field(self, name, uri_reconciliator, id_entity):
        """
        The actual function that changes the name format to the one required for visualization

        :name: entity name
        :uri_reconciliator: the URI of the affiliated knowledge graph
        :id_entity: entity ID
        :return: the name in the correct format
        """
        return {
            'value': name,
            'uri': f"{uri_reconciliator}{id_entity}"
        }


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
                entity['name'] = self.parse_name_field(
                    entity.get('name', ''),  # Safely access 'name'
                    uri_reconciliator,
                    entity_type
                )
        return entities

    def get_reconciliator(self, id_reconciliator, response):
        """
        Function that, given the reconciliator's ID, returns a dictionary 
        with all the service information

        :id_reconciliator: the ID of the reconciliator in question
        :return: a dictionary with the reconciliator's information
        """
        for reconciliator in response:
            if reconciliator['id'] == id_reconciliator:
                return {
                    'uri': reconciliator['uri'],
                    'prefix': reconciliator['prefix'],
                    'name': reconciliator['name'],
                    'relativeUrl': reconciliator['relativeUrl']
                }
        return None


    def get_reconciliator_data(self):
        """
        Retrieves reconciliator data from the backend.
        :return: data of reconciliator services in JSON format
        """
        try:
            response = requests.get(f"{self.api_url}reconciliators/list", headers=self.headers)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while retrieving reconciliator data: {e}")
            return None

    def create_annotation_meta_cell(self, metadata):
        """
        Creates the annotationMeta field at the cell level, 
        which will then be inserted into the table

        :metadata: cell-level metadata
        :return: the dictionary with data regarding annotationMeta
        """
        score_bound = self.calculate_score_bound_cell(metadata)
        return {'annotated': True,
                'match': {'value': self.value_match_cell(metadata)},
                'lowestScore': score_bound['lowestScore'],
                'highestScore': score_bound['highestScore']}


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

            uri_reconciliator = self.get_reconciliator(id_reconciliator, reconciliator_response)['uri']
            new_cell['metadata'] = self.parse_name_entities(new_cell['metadata'], uri_reconciliator)

            if column_type == 'entity':
                new_cell['annotationMeta'] = self.create_annotation_meta_cell(new_cell['metadata'])
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

    def extend_reconciled_ColumnExt(self, table, reconciliated_column_name, properties):
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

    def process_format_and_construct_payload_reconciledColumnExt(self, reconciled_json, extended_json, reconciliated_column_name, properties):
        """
        Processes the format and constructs the payload for reconciledColumnExt.

        :param reconciled_json: the original reconciled JSON
        :param extended_json: the extended JSON
        :param reconciliated_column_name: the column containing the ID in the KG
        :param properties: the properties to extend in the table
        :return: constructed payload
        """
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
                    if new_cell_id in extended_json['rows'][row_id]['cells']:
                        row['cells'][new_cell_id] = {
                            'id': f"{row_id}${new_cell_id}",
                            'label': extended_json['rows'][row_id]['cells'][new_cell_id]['label'],
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

    def extend_column(self, table, reconciliated_column_name, id_extender, properties, date_column_name=None, decimal_format=None):
        """
        Extends the specified properties present in the Knowledge Graph as new columns and constructs the payload.

        :param table: the table containing the data (this will be used as reconciled_json)
        :param reconciliated_column_name: the column containing the ID in the KG
        :param id_extender: the extender to use for extension
        :param properties: the properties to extend in the table
        :param date_column_name: the name of the date column to extract date information for each row
        :param decimal_format: the decimal format to use for the values (default: None)
        :return: tuple (extended_table, extension_payload)
        """
        try:
            if id_extender == "reconciledColumnExt":
                extended_table = self.extend_reconciled_ColumnExt(table, reconciliated_column_name, properties)
                extension_payload = self.process_format_and_construct_payload_reconciledColumnExt(
                    reconciled_json=table,
                    extended_json=extended_table,
                    reconciliated_column_name=reconciliated_column_name,
                    properties=properties
                )
            elif id_extender == "meteoPropertiesOpenMeteo":
                extended_table, extension_payload = self.extend_meteo_properties(table, reconciliated_column_name, properties, date_column_name, decimal_format)
            else:
                raise ValueError(f"Unsupported extender_id: {id_extender}")

            if extended_table is None:
                print("Failed to extend table.")
                return None, None

            return extended_table, extension_payload
        except Exception as e:
            print(f"Error in extend_column: {str(e)}")
            import traceback
            traceback.print_exc()
            return None, None
    
    def extend_reconciled_column(self, table, reconciliated_column_name, properties):
        """
        Extends the reconciled column with specified properties.

        :param table: the table containing the data
        :param reconciliated_column_name: the column containing the ID in the KG
        :param properties: the properties to extend in the table
        :return: extended table
        """
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

    def get_reconciliator_data(self):
        """
        Retrieves reconciliator data from the backend.
        :return: data of reconciliator services in JSON format
        """
        try:
            response = requests.get(f"{self.api_url}reconciliators/list", headers=self.headers)
            response.raise_for_status()  # Raise an exception for 4xx or 5xx status codes
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while retrieving reconciliator data: {e}")
            return None

    def extend_meteo_properties(self, table, reconciliated_column_name, properties, date_column_name, separator_format):
        """
        Extends the table with meteo properties.

        :param table: the table containing the data
        :param reconciliated_column_name: the column containing the ID in the KG
        :param properties: the properties to extend in the table
        :param date_column_name: the name of the date column to extract date information for each row
        :param separator_format: the decimal format to use for the values ("comma" or "default")
        :return: tuple (extended_table, extension_payload)
        """
        reconciliator_response = self.get_reconciliator_data()
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

        payload = self.create_extension_payload(table, reconciliated_column_name, properties, "meteoPropertiesOpenMeteo", dates, properties, separator_format)

        headers = {"Accept": "application/json"}

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Apply comma separator format conversion only if specified as "comma"
            if separator_format.lower() == "comma":
                data = self.convert_to_comma_separator(data)
            # If separator_format is "default" or any other value, we don't modify the data
            
            # Remove brackets from labels
            data = self.remove_brackets_from_labels(data)
            
            extended_table = self.add_extended_columns(table, data, properties, reconciliator_response)
            
            # Create the extension payload
            extension_payload = self.process_format_and_construct_payload(
                reconciled_json=table,
                extended_json=extended_table,
                reconciliated_column_name=reconciliated_column_name,
                properties=properties
            )
            
            return extended_table, extension_payload
        except requests.RequestException as e:
            print(f"An error occurred while making the request: {e}")
            return None, None
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON response: {e}")
            return None, None
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return None, None

    def remove_brackets_from_labels(self, data):
        """
        Removes brackets from labels in the response data.
        """
        for row in data['rows'].values():
            for cell in row['cells'].values():
                if isinstance(cell['label'], list) and len(cell['label']) == 1:
                    cell['label'] = cell['label'][0]
        return data
    
    def convert_to_comma_separator(self, data):
        """
        Converts decimal separator from dot to comma in the response data.
        """
        for column in data['columns'].values():
            for cell in column['cells'].values():
                if isinstance(cell['label'], list):
                    cell['label'] = [
                        str(value).replace('.', ',') if isinstance(value, (float, int)) else value
                        for value in cell['label']
                    ]
        return data
   
    def process_format_and_construct_payload(self, reconciled_json, extended_json, reconciliated_column_name, properties):
        """
        Processes the format and constructs the payload.

        :param reconciled_json: the original reconciled JSON
        :param extended_json: the extended JSON
        :param reconciliated_column_name: the column containing the ID in the KG
        :param properties: the properties to extend in the table
        :return: constructed payload
        """
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
    
    def create_payload_ColumnExt(self, table, reconciliated_column_name, properties):
        rows = table['rows']
        items = {}
        column = {}
        
        for row_id, row_data in rows.items():
            cell = row_data['cells'][reconciliated_column_name]
            cell_label = cell['label']
            cell_metadata = cell['metadata'][0]
            
            items.setdefault(reconciliated_column_name, {})[row_id] = cell_metadata['id']
            column[row_id] = [
                cell_label,
                [cell_metadata],
                reconciliated_column_name
            ]
        
        return {
            "serviceId": "reconciledColumnExt",
            "items": items,
            "column": column,
            "property": properties
        }

    def send_payload_ColumnExt(self, payload):
        url = urljoin(self.api_url, 'extenders')
        response = requests.post(url, headers=self.headers, data=json.dumps(payload))
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Request failed with status code: {response.status_code}. Response: {response.text}")

    def merge_data_ColumnExt(self, input_data, api_response, reconciliated_column_name, properties):
        output = deepcopy(input_data)
        
        # Update table data
        new_columns = [col for col in api_response.get('columns', {}) if col not in output['columns']]
        output['table']['nCols'] += len(new_columns)
        output['table']['nCells'] = output['table']['nRows'] * output['table']['nCols']
        output['table']['nCellsReconciliated'] = len(output['rows'])
        output['table']['minMetaScore'] = 0
        output['table']['maxMetaScore'] = 1

        # Update reconciliated column
        output['columns'][reconciliated_column_name]['status'] = 'reconciliated'
        output['columns'][reconciliated_column_name]['annotationMeta'] = {
            'annotated': True,
            'match': {'value': True, 'reason': 'reconciliator'},
            'lowestScore': 0,
            'highestScore': 0
        }

        # Add new columns from API response
        for new_col in new_columns:
            output['columns'][new_col] = {
                'id': new_col,
                'label': new_col,
                'metadata': [],
                'status': 'empty',
                'context': {}
            }

        # Update rows
        for row_id, row_data in output['rows'].items():
            cell = row_data['cells'][reconciliated_column_name]
            cell_metadata = cell['metadata'][0]
            
            # Update reconciliated cell
            cell['annotationMeta'] = {
                'annotated': True,
                'match': {'value': True, 'reason': 'reconciliator'},
                'lowestScore': 1,
                'highestScore': 1
            }
            
            # Add new cells from API response
            for new_col in new_columns:
                if row_id in api_response['columns'][new_col].get('cells', {}):
                    row_data['cells'][new_col] = {
                        'id': f"{row_id}${new_col}",
                        'label': api_response['columns'][new_col]['cells'][row_id]['label'],
                        'metadata': []
                    }
                else:
                    # If the expected data is not in the API response, add an empty cell
                    row_data['cells'][new_col] = {
                        'id': f"{row_id}${new_col}",
                        'label': '',
                        'metadata': []
                    }

        # Create payload for backend
        backend_payload = {
            "tableInstance": {
                "id": output['table']['id'],
                "idDataset": output['table']['idDataset'],
                "name": output['table']['name'],
                "nCols": output['table']['nCols'],
                "nRows": output['table']['nRows'],
                "nCells": output['table']['nCells'],
                "nCellsReconciliated": output['table']['nCellsReconciliated'],
                "lastModifiedDate": output['table']['lastModifiedDate'],
                "minMetaScore": output['table'].get('minMetaScore', 0),
                "maxMetaScore": output['table'].get('maxMetaScore', 1)
            },
            "columns": {
                "byId": output['columns'],
                "allIds": list(output['columns'].keys())
            },
            "rows": {
                "byId": output['rows'],
                "allIds": list(output['rows'].keys())
            }
        }

        return output, backend_payload
    
    def extend_reconciledColumnExt(self, table, reconciliated_column_name, id_extender, properties):
        try:
            payload = self.create_payload_ColumnExt(table, reconciliated_column_name, properties)
            api_response = self.send_payload_ColumnExt(payload)
            print("API Response:", json.dumps(api_response, indent=2))  # Debug print
            
            if not api_response.get('columns'):
                raise ValueError("API response does not contain 'columns' key")
            
            extended_table, backend_payload = self.merge_data_ColumnExt(table, api_response, reconciliated_column_name, properties)
            return extended_table, backend_payload
        except Exception as e:
            print(f"Error in extend_reconciledColumnExt: {str(e)}")
            return None, None
     