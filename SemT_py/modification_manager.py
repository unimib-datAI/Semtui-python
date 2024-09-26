import pandas as pd

class ModificationManager:
    @staticmethod
    def iso_date(df, date_col):
        # Check if the column exists in the DataFrame
        if date_col not in df.columns:
            raise ValueError(f"Column '{date_col}' does not exist in the DataFrame.")
        
        # Check if the column is already in datetime format
        if pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
            return df
        
        # Attempt to parse the column as dates
        try:
            df[date_col] = pd.to_datetime(df[date_col], format='%Y%m%d', errors='coerce')
        except Exception as e:
            raise ValueError(f"Error parsing column '{date_col}' as dates: {e}")
        
        # Check for any NaT values resulting from failed conversions
        if df[date_col].isnull().any():
            raise ValueError(f"Column '{date_col}' contains invalid date values that could not be converted.")
        
        # Convert to ISO format
        df[date_col] = df[date_col].dt.strftime('%Y-%m-%d')
        return df

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
