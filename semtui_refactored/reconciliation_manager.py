import requests
import json
import pandas as pd
from .token_manager import TokenManager

class ReconciliationManager:
    def __init__(self, api_url, token):
        self.api_url = api_url
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

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

    def get_reconciliators_list(self):
        """
        Provides a list of available reconciliators with their main information.
        :return: DataFrame containing reconciliators and their information
        """
        response = self.get_reconciliator_data()
        if response:
            return self.clean_service_list(response)
        return None

    def get_extender_data(self):
        """
        Retrieves extender data from the backend

        :return: data of extension services in JSON format
        """
        try:
            response = requests.get(f"{self.api_url}extenders/list", headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while retrieving extender data: {e}")
            return None

    def get_extenders_list(self):
        """
        Provides a list of available extenders with their main information

        :return: a dataframe containing extenders and their information
        """
        response = self.get_extender_data()
        if response:
            return self.clean_service_list(response)
        return None

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

    def create_reconciliation_payload(self, table, column_name, id_reconciliator):
        """
        Creates the payload for the reconciliation request

        :table: table in raw format
        :columnName: the name of the column to reconcile
        :idReconciliator: the id of the reconciliation service to use
        :return: the request payload
        """
        rows = []
        rows.append({"id": 'column$index', "label": column_name})
        for row in table['rows'].keys():
            rows.append({"id": row+"$"+column_name,
                        "label": table['rows'][row]['cells'][column_name]['label']})
        return {"serviceId": id_reconciliator, "items": rows}

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
            'uri': uri_reconciliator + id_entity
        }

    def create_cell_metadata_name_field(self, metadata, id_reconciliator, reconciliator_response):
        """
        Refactor of the name field within cell-level metadata
        necessary for visualization within SEMTUI

        :metadata: column-level metadata
        :id_reconciliator: ID of the reconciliator performed in the operation
        :reconciliator_response: response containing reconciliator information
        :return: metadata containing the name field in the new format
        """
        for row in range(len(metadata)):
            try:
                for item in range(len(metadata[row]["metadata"])):
                    value = metadata[row]["metadata"][item]['name']
                    uri = metadata[row]["metadata"][item]['id']
                    metadata[row]["metadata"][item]['name'] = self.parse_name_field(
                        value, self.get_reconciliator(id_reconciliator, reconciliator_response)['uri'], uri.split(':')[1])
            except:
                return []
        return metadata

    def calculate_score_bound_cell(self, metadata):
        """
        Calculates the min and max value of the score of the results obtained for
        a single cell

        :metadata: metadata of a single cell
        :return: a dictionary containing the two values
        """
        try:
            score_list = [item['score'] for item in metadata]
            return {'lowestScore': min(score_list), 'highestScore': max(score_list)}
        except:
            return {'lowestScore': 0, 'highestScore': 0}
    
    def value_match_cell(self, metadata):
        """
        Returns whether a cell has obtained a match or not

        :metadata: cell-level metadata
        :return: True or False based on the match occurrence
        """
        for item in metadata:
            if item['match'] == True:
                return True
        return False

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

    def update_metadata_cells(self, table, metadata):
        """
        Allows inserting new cell-level metadata

        :table: table in raw format
        :metadata: cell-level metadata
        :return: the table in raw format with metadata
        """
        for item in metadata:
            item["id"] = item["id"].split("$")
            try:
                table["rows"][item["id"][0]]["cells"][item["id"]
                                                    [1]]["metadata"] = item["metadata"]
                table["rows"][item["id"][0]]["cells"][item["id"][1]
                                                    ]["annotationMeta"] = self.create_annotation_meta_cell(item["metadata"])
            except:
                print("")
        return table

    def calculate_n_cells_reconciliated_column(self, table, column_name):
        """
        Calculates the number of reconciled cells within 
        a column

        :table: table in raw format
        :column_name: name of the column in question
        :return: the number of reconciled cells
        """
        cells_reconciliated = 0
        rows_index = table["rows"].keys()
        for row in rows_index:
            try:
                if table['rows'][row]['cells'][column_name]['annotationMeta']["annotated"] == True:
                    cells_reconciliated += 1
            except:
                cells_reconciliated = cells_reconciliated
        return cells_reconciliated

    def create_context_column(self, table, column_name, id_reconciliator, reconciliator_response):
        """
        Creates the context field at the column level by retrieving the necessary data

        :table: table in raw format
        :column_name: the name of the column for which the context is being created
        :id_reconciliator: the ID of the reconciliator used for the column
        :reconciliator_response: response containing reconciliator information
        :return: the context field of the column
        """
        n_cells = len(table["rows"].keys())
        reconciliator = self.get_reconciliator(id_reconciliator, reconciliator_response)
        return {reconciliator['prefix']: {
                'uri': reconciliator['uri'],
                'total': n_cells,
                'reconciliated': self.calculate_n_cells_reconciliated_column(table, column_name)
                }}

    def get_column_metadata(self, metadata):
        """
        Allows retrieving column-level data, particularly
        the entity corresponding to the column, the column types,
        and the match value of the entities in the column

        :metadata: column metadata obtained from the reconciliator
        :return: dictionary containing the different data
        """
        entity = []
        types = []
        for i in range(len(metadata)):
            try:
                if metadata[i]['id'] == ['column', 'index']:
                    entity = metadata[i]['metadata']
            except:
                print("No column entity is provided")
            try:
                if metadata[i]['id'] != ['column', 'index']:
                    for j in range(len(metadata[i]['metadata'])):
                        if metadata[i]['metadata'][j]['match'] == True:
                            types.append(metadata[i]['metadata'][j]['type'][0])
            except:
                print("No column type is provided")
        match_metadata_value = True
        for item in entity:
            if item['match'] == False:
                match_metadata_value = False
        return {'entity': entity, 'type': types, 'matchMetadataValue': match_metadata_value}

    def create_metadata_field_column(self, metadata):
        """
        Allows creating the metadata field for a column, which will
        then be inserted into the general column-level metadata

        :metadata: column-level metadata
        :return: the metadata field at the column level
        """
        return [
            {'id': '',
            'match': self.get_column_metadata(metadata)['matchMetadataValue'],
            'score': 0,
            'name':{'value': '', 'uri': ''},
            'entity': self.get_column_metadata(metadata)['entity'],
            'property':[],
            'type': self.get_column_metadata(metadata)['type']}
        ]

    def calculate_score_bound_column(self, table, column_name, reconciliator_response):
        all_scores = []
        match_value = True
        rows = table["rows"].keys()
        for row in rows:
            try:
                annotation_meta = table["rows"][row]['cells'][column_name]['annotationMeta']
                if annotation_meta['annotated'] == True:
                    all_scores.append(annotation_meta['lowestScore'])
                    all_scores.append(annotation_meta['highestScore'])
                if annotation_meta['match']['value'] == False:
                    match_value = False
            except KeyError:
                print(f"Missing key in cell annotation metadata: 'annotationMeta'")
                print(f"Row: {row}, Column: {column_name}")
                print(f"Cell data: {table['rows'][row]['cells'][column_name]}")
        
        if all_scores:
            return {'lowestScore': min(all_scores), 'highestScore': max(all_scores), 'matchValue': match_value}
        else:
            print("No valid annotation metadata found for the column.")
            return {'lowestScore': None, 'highestScore': None, 'matchValue': None}

    def create_annotation_meta_column(self, annotated, table, column_name, reconciliator_response):
        score_bound = self.calculate_score_bound_column(
            table, column_name, reconciliator_response)
        return {'annotated': annotated,
                'match': {'value': score_bound['matchValue']},
                'lowestScore': score_bound['lowestScore'],
                'highestScore': score_bound['highestScore']
                }

    def update_metadata_column(self, table, column_name, id_reconciliator, metadata, reconciliator_response):
        """
        Allows inserting column-level metadata

        :table: table in raw format
        :column_name: name of the column to operate on
        :id_reconciliator: ID of the reconciliator used
        :metadata: column-level metadata
        :reconciliator_response: response containing reconciliator information
        :return: the table with the new metadata inserted
        """
        # inquire about the different states
        table['columns'][column_name]['status'] = 'pending'
        table['columns'][column_name]['kind'] = "entity"
        table['columns'][column_name]['context'] = self.create_context_column(
            table, column_name, id_reconciliator, reconciliator_response)
        table['columns'][column_name]['metadata'] = self.create_metadata_field_column(
            metadata)
        table['columns'][column_name]['annotationMeta'] = self.create_annotation_meta_column(
            True, table, column_name, reconciliator_response)
        return table

    def update_metadata_table(self, table):
        """
        Updates the table-level metadata.

        :param table: table in raw format
        :return: updated table
        """
        # Placeholder implementation
        return table

    def extend_column(self, table, reconciliated_column_name, id_extender, properties, date_column_name=None, weather_params=None, decimal_format=None):
        """
        Extends the specified properties present in the Knowledge Graph as new columns.

        :param table: the table containing the data
        :param reconciliated_column_name: the column containing the ID in the KG
        :param id_extender: the extender to use for extension
        :param properties: the properties to extend in the table
        :param date_column_name: the name of the date column to extract date information for each row
        :param weather_params: a list of weather parameters to include in the request
        :param decimal_format: the decimal format to use for the values (default: None)
        :return: the extended table
        """
        if id_extender == "reconciledColumnExt":
            # Simplified local extension for reconciledColumnExt
            for prop in properties:
                new_column_name = f"{prop}_{reconciliated_column_name}"
                table['columns'][new_column_name] = {
                    'id': new_column_name,
                    'label': new_column_name,
                    'status': 'empty',
                    'context': {},
                    'metadata': []
                }
                for row_key, row_data in table['rows'].items():
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
            return table
        else:
            # Existing logic for other extenders
            reconciliator_response = self.reconciliation_manager.get_reconciliator_data()
            extender_data = self.get_extender(id_extender, self.get_extender_data())
            
            if extender_data is None:
                raise ValueError(f"Extender with ID '{id_extender}' not found.")
            
            url = self.api_url + "extenders/" + extender_data['relativeUrl']
            
            # Prepare the dates information dynamically
            dates = {}
            for row_key, row_data in table['rows'].items():
                date_value = row_data['cells'].get(date_column_name, {}).get('label')
                if date_value:
                    dates[row_key] = [date_value]
                else:
                    print(f"Missing or invalid date for row {row_key}, skipping this row.")
                    continue  # Optionally skip this row or handle accordingly
            decimal_format = ["comma"]  # Use comma as the decimal separator
            payload = self.create_extension_payload(table, reconciliated_column_name, properties, id_extender, dates, weather_params, decimal_format)
            
            headers = {"Accept": "application/json"}

            try:
                response = requests.post(url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                table = self.add_extended_columns(table, data, properties, reconciliator_response)
                return table
            except requests.RequestException as e:
                print(f"An error occurred while making the request: {e}")
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON response: {e}")
            except Exception as e:
                print(f"An unexpected error occurred: {e}")

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
    
    def push_reconciliation_data_to_backend(self, dataset_id, table_id, reconciled_data):
        """
        Pushes reconciliation data to the backend.

        :param dataset_id: ID of the dataset
        :param table_id: ID of the table
        :param reconciled_data: Reconciled data to be sent
        :return: Response from the backend
        """
        # Create the endpoint URL
        url = f"{self.api_url}dataset/{dataset_id}/table/{table_id}"

        # Create the update payload
        update_payload = self.create_reconciliation_payload_for_backend(reconciled_data)

        try:
            # Send the PUT request to update the table
            response = requests.put(url, headers=self.headers, json=update_payload)
            
            if response.status_code == 200:
                print("Table updated successfully!")
                return response.json()
            elif response.status_code == 401:
                print("Unauthorized: Invalid or missing token.")
            elif response.status_code == 404:
                print(f"Dataset or table with ID {dataset_id}/{table_id} not found.")
            else:
                print(f"Failed to update table: {response.status_code}, {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Error occurred while updating table: {e}")
            return None
    
    def create_reconciliation_payload_for_backend(self, table_json):
        """
        Creates the payload required to perform the table update operation.

        :param table_json: JSON representation of the table
        :return: request payload
        """
        payload = {
            "tableInstance": {
                "id": table_json["table"]["id"],
                "idDataset": table_json["table"]["idDataset"],
                "name": table_json["table"]["name"],
                "nCols": table_json["table"]["nCols"],
                "nRows": table_json["table"]["nRows"],
                "nCells": table_json["table"]["nCells"],
                "nCellsReconciliated": table_json["table"]["nCellsReconciliated"],
                "lastModifiedDate": table_json["table"]["lastModifiedDate"]
            },
            "columns": {
                "byId": table_json["columns"],
                "allIds": list(table_json["columns"].keys())
            },
            "rows": {
                "byId": table_json["rows"],
                "allIds": list(table_json["rows"].keys())
            }
        }
        return payload
