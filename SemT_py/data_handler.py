import pandas as pd
import chardet
import csv

class DataHandler:
    """ A class for reading data files with automatic encoding detection and delimiter inference. """
    
    def __init__(self, file_path):
        """
        Initializes the DataHandler object with the given file path.

        Args:
            file_path (str): The path to the file to be read.
        """
        self.file_path = file_path

    def read_csv_data(self, delimiter=None):
        """
        Reads a CSV file with automatic encoding detection and delimiter inference.

        Args:
            delimiter (str, optional): The delimiter used in the CSV file. If not provided, it will be inferred.

        Returns:
            pd.DataFrame: The DataFrame containing the contents of the CSV file.

        Raises:
            FileNotFoundError: If the file is not found.
            PermissionError: If the file cannot be accessed due to permission issues.
            Exception: If any other error occurs during file reading.
        """
        try:
            # Detect the encoding of the file
            with open(self.file_path, 'rb') as file:
                encoding = chardet.detect(file.read())['encoding']

            # Read the CSV file with pandas using the detected encoding
            if delimiter is None:
                # If delimiter is not provided, try to infer it
                sniffer = csv.Sniffer()
                with open(self.file_path, 'r', encoding=encoding) as file:
                    sample = file.read(1024)  # Read a sample of the file
                    delimiter = sniffer.sniff(sample).delimiter
            
            df = pd.read_csv(self.file_path, sep=delimiter, encoding=encoding)
            print(f"File '{self.file_path}' read successfully with encoding '{encoding}' and delimiter '{delimiter}'")
            return df

        except FileNotFoundError:
            print(f"File '{self.file_path}' not found.")
            raise
        except PermissionError:
            print(f"Permission denied to read file '{self.file_path}'.")
            raise
        except Exception as e:
            print(f"Error reading file '{self.file_path}': {str(e)}")
            raise
