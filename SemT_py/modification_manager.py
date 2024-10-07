import pandas as pd
from dateutil import parser
import re

class ModificationManager:
    @staticmethod
    def iso_date(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
        """
        Convert a given date column in a DataFrame to ISO 8601 date format (YYYY-MM-DD).
        If the column is already formatted correctly, no changes are made.
        
        Parameters:
        - df (pd.DataFrame): Input DataFrame containing the date column.
        - date_col (str): Name of the date column to be converted.

        Returns:
        - pd.DataFrame: DataFrame with the date column converted to ISO 8601 date format.
        - str: Message indicating if any changes were made or if the column was already formatted.

        Raises:
        - ValueError: If the column does not exist or contains invalid dates.
        """
        # Check if the column exists in the DataFrame
        if date_col not in df.columns:
            raise ValueError(f"Column '{date_col}' does not exist in the DataFrame.")
        
        # Regular expression pattern to match ISO 8601 date format (YYYY-MM-DD)
        iso_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')

        # Check if all values in the column match the ISO date pattern
        if df[date_col].apply(lambda x: bool(iso_pattern.match(str(x)))).all():
            return df, "Input is already formatted correctly as ISO 8601 (YYYY-MM-DD)."

        # Function to parse dates using dateutil.parser and handle errors
        def parse_date_safe(date_str):
            try:
                parsed_date = parser.parse(str(date_str), fuzzy=True)
                return parsed_date.strftime('%Y-%m-%d')  # Return date in 'YYYY-MM-DD' format
            except (ValueError, TypeError):
                return None

        # Apply date parsing with error handling
        df[date_col] = df[date_col].apply(parse_date_safe)

        # Check for any NaT values resulting from failed conversions
        if df[date_col].isnull().any():
            invalid_rows = df[df[date_col].isnull()].index.tolist()
            raise ValueError(f"Column '{date_col}' contains invalid date values that could not be converted. "
                             f"Invalid rows: {invalid_rows}")

        return df, "Date column successfully converted to ISO 8601 format."
    
    @staticmethod
    def lower_case(df, column):
        # Check if the column exists in the DataFrame
        if column not in df.columns:
            raise ValueError(f"Column '{column}' does not exist in the DataFrame.")
        
        # Ensure the column is of string type
        if not pd.api.types.is_string_dtype(df[column]):
            raise ValueError(f"Column '{column}' is not of string type.")
        
        df[column] = df[column].str.lower()
        return df

    @staticmethod
    def drop_na(df):
        df.dropna(inplace=True)
        return df

    @staticmethod
    def rename_columns(df, column_rename_dict):
        # Check if all columns to be renamed exist in the DataFrame
        missing_cols = [col for col in column_rename_dict.keys() if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} do not exist in the DataFrame.")
        
        df = df.rename(columns=column_rename_dict)
        return df

    @staticmethod
    def convert_dtypes(df, dtype_dict):
        for col, dtype in dtype_dict.items():
            # Check if the column exists in the DataFrame
            if col not in df.columns:
                raise ValueError(f"Column '{col}' does not exist in the DataFrame.")
            
            try:
                df[col] = df[col].astype(dtype)
            except Exception as e:
                raise ValueError(f"Error converting column '{col}' to type '{dtype}': {e}")
        return df

    @staticmethod
    def reorder_columns(df, new_column_order):
        # Check if all specified columns exist in the DataFrame
        missing_cols = [col for col in new_column_order if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Columns {missing_cols} do not exist in the DataFrame.")
        
        df = df[new_column_order]
        return df
