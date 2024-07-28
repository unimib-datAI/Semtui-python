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

    def send_extension_request(self, payload):
        response = requests.post(self.api_url, headers=self.headers, json=payload)
        response.raise_for_status()
        return response.json()

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
        input_data = self.prepare_input_data(table, reconciliated_column_name, id_extender, properties)
        extension_response = self.send_extension_request(input_data)
        extended_table = self.compose_extension_table(table, extension_response)
        backend_payload = self.create_backend_payload(extended_table)
        return extended_table, backend_payload
