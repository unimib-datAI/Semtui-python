import requests
import json
import copy
import datetime
from urllib.parse import urljoin

class ReconciliationManager:
    def __init__(self, base_url, token):
        self.base_url = base_url.rstrip('/') + '/'
        self.api_url = urljoin(self.base_url, 'api/')
        self.token = token
        self.headers = {
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json',
            'Accept': 'application/json'
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
                for optional_column in optional_columns:
                    optional_value = row_data['cells'][optional_column]['label']
                    input_data['secondPart'][row_id] = [optional_value, [], optional_column]

        return input_data

    def send_reconciliation_request(self, input_data, reconciliator_id):
        url = urljoin(self.api_url, f'reconciliators/{reconciliator_id}')
        response = requests.post(url, json=input_data, headers=self.headers)

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            return None

    def compose_reconciled_table(self, original_input, reconciliation_output, column_name, reconciliator_id):
        final_payload = copy.deepcopy(original_input)

        final_payload['table']['id'] = str(int(final_payload['table']['id']) + 1)
        final_payload['table']['name'] = f"New_JOT_tiny_{datetime.datetime.now().strftime('%d%m%Y')}_{int(final_payload['table']['id'])}"
        final_payload['table']['lastModifiedDate'] = datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

        final_payload['columns'][column_name]['status'] = 'pending'
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
        final_payload['columns'][column_name]['metadata'] = [{
            'id': '',
            'match': True,
            'score': 0,
            'name': {'value': '', 'uri': ''},
            'entity': column_metadata['metadata'],
            'property': [],
            'type': [{'id': 'wd:Q29934236', 'name': 'GlobeCoordinate'}] * 3
        }]

        for item in reconciliation_output:
            if item['id'] != column_name:
                row_id, cell_id = item['id'].split('$')
                cell = final_payload['rows'][row_id]['cells'][cell_id]

                metadata = item['metadata'][0]
                cell['metadata'] = [{
                    'id': metadata['id'],
                    'feature': metadata.get('feature', []),
                    'name': {
                        'value': metadata['name'],
                        'uri': f"http://www.google.com/maps/place/{metadata['id'].split(':')[1]}"
                    },
                    'score': metadata['score'],
                    'match': metadata['match'],
                    'type': metadata['type']
                }]

                cell['annotationMeta'] = {
                    'annotated': True,
                    'match': {'value': True},
                    'lowestScore': metadata['score'],
                    'highestScore': metadata['score']
                }

        final_payload['id'] = final_payload['table']['id']

        return final_payload

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

    def reconcile(self, table_data, column_name, reconciliator_id, optional_columns):
        if reconciliator_id not in ['geocodingHere', 'geocodingGeonames', 'geonames']:
            raise ValueError("Invalid reconciliator ID. Please use 'geocodingHere', 'geocodingGeonames', or 'geonames'.")

        input_data = self.prepare_input_data(table_data, column_name, reconciliator_id, optional_columns)
        response_data = self.send_reconciliation_request(input_data, reconciliator_id)

        if response_data:
            final_payload = self.compose_reconciled_table(table_data, response_data, column_name, reconciliator_id)
            backend_payload = self.create_backend_payload(final_payload)
            return final_payload, backend_payload
        else:
            return None, None
