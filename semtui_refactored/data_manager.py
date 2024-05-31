from .token_manager import TokenManager
from .file_reader import FileReader
import pandas as pd

class DataManager:
    def __init__(self, api_url, username, password):
        self.api_url = api_url
        signin_data = {"username": username, "password": password}
        signin_headers = {
            "accept": "application/json",
            "content-type": "application/json"
        }
        self.token_manager = TokenManager(self.api_url, signin_data, signin_headers)
        self.token = self.token_manager.get_token()

    def obtain_auth_token(self):
        return self.token

    def read_csv_data(self, file_path):
        file_reader = FileReader(file_path)
        return file_reader.read_csv()

    def process_data(self, df, date_col=None, lowercase_col=None, dropna=False, column_rename_dict=None, dtype_dict=None, new_column_order=None):
        # If a date column is specified, convert it to ISO format
        if date_col:
            df[date_col] = pd.to_datetime(df[date_col], format='%Y%m%d')
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')

        # If a column for lowercase conversion is specified, convert it
        if lowercase_col:
            df[lowercase_col] = df[lowercase_col].str.lower()

        # If dropna is True, drop null values
        if dropna:
            df.dropna(inplace=True)

        # Rename columns if column_rename_dict is provided
        if column_rename_dict:
            df = df.rename(columns=column_rename_dict)

        # Convert data types if dtype_dict is provided
        if dtype_dict:
            for col, dtype in dtype_dict.items():
                df[col] = df[col].astype(dtype)

        # Reorder columns if new_column_order is provided
        if new_column_order:
            df = df[new_column_order]

        # Add more transformations as needed

        return df