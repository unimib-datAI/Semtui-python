import pandas as pd
from .token_manager import TokenManager

class EvaluationManager:
    def __init__(self):
        pass

    def extract_row_metadata(self, reconciled_table, row_id, reconciled_columns):
        """
        Extracts metadata for specific columns in a given row.

        :param reconciled_table: The reconciled table data.
        :param row_id: The ID of the row to extract metadata from.
        :param reconciled_columns: A list of column names to extract metadata from.
        :return: A dictionary containing metadata for the specified columns.
        """
        if 'raw' not in reconciled_table or 'rows' not in reconciled_table['raw']:
            raise ValueError("The 'reconciled_table' object does not have the expected structure.")
        
        rows = reconciled_table['raw']['rows']
        
        if row_id not in rows:
            raise ValueError(f"Row with ID '{row_id}' not found in the reconciled table.")
        
        row_metadata = {}
        
        for column_name in reconciled_columns:
            if column_name not in rows[row_id]['cells']:
                print(f"Warning: Column '{column_name}' not found in row '{row_id}'. Skipping.")
                continue
            
            cell = rows[row_id]['cells'][column_name]
            
            if 'metadata' in cell:
                row_metadata[column_name] = cell['metadata']
            else:
                print(f"Warning: No metadata found for column '{column_name}' in row '{row_id}'.")
        
        return row_metadata

    def count_cells_with_label(self, table, columns):
        """
        Counts cells with labels for specified columns.

        :param table: The table data.
        :param columns: A list of column names to count cells for.
        :return: A dictionary with column names as keys and counts as values.
        """
        column_counts = {}
        for column_name in columns:
            count = 0
            for row in table['rows'].values():
                cell = row['cells'].get(column_name)
                if cell and cell.get('label'):
                    count += 1
            column_counts[column_name] = count
        return column_counts

    def count_unique_values(self, table, columns, key='label'):
        """
        Counts unique values in specified columns.

        :param table: The table data.
        :param columns: A list of column names to count unique values for.
        :param key: The key to use for counting unique values ('label' or 'metadata').
        :return: A dictionary with column names as keys and counts of unique values as values.
        """
        column_unique_counts = {}
        for column_name in columns:
            unique_values = set()
            for row in table['rows'].values():
                cell = row['cells'].get(column_name)
                if cell and cell.get(key):
                    if key == 'metadata':
                        for metadata in cell['metadata']:
                            unique_values.add(metadata['id'])
                    else:
                        unique_values.add(cell[key])
            column_unique_counts[column_name] = len(unique_values)
        return column_unique_counts

    def calculate_percentages(self, table, columns):
        """
        Calculates the percentage of cells with labels for specified columns.

        :param table: The table data.
        :param columns: A list of column names to calculate percentages for.
        :return: A dictionary with column names as keys and percentages as values.
        """
        total_rows = len(table['rows'])
        column_percentages = {}
        for column_name in columns:
            count = 0
            for row in table['rows'].values():
                cell = row['cells'].get(column_name)
                if cell and cell.get('label'):
                    count += 1
            percentage = (count / total_rows) * 100
            column_percentages[column_name] = percentage
        return column_percentages

    # Specific evaluation functions using the common logic
    def count_extended_cells_per_column(self, extended_table, extended_columns):
        return self.count_cells_with_label(extended_table, extended_columns)

    def count_unique_extended_values_per_column(self, extended_table, extended_columns):
        return self.count_unique_values(extended_table, extended_columns, key='label')

    def percentage_extended_cells_per_column(self, extended_table, extended_columns):
        return self.calculate_percentages(extended_table, extended_columns)

    def count_reconciled_cells_per_column(self, reconciled_table, reconciled_columns):
        return self.count_cells_with_label(reconciled_table, reconciled_columns)

    def count_unique_reconciled_values_per_column(self, reconciled_table, reconciled_columns):
        return self.count_unique_values(reconciled_table, reconciled_columns, key='metadata')

    def percentage_reconciled_cells_per_column(self, reconciled_table, reconciled_columns):
        return self.calculate_percentages(reconciled_table, reconciled_columns)
