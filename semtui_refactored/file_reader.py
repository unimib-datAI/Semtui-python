import pandas as pd
import chardet

class FileReader:
    """ A class for reading files with detected encoding and delimiter. """
    def __init__(self, file_path):
        """ Initializes the FileReader object with the given file path.

        Args:
            file_path (str): The path to the file to be read.
        """
        self.file_path = file_path

    def read_csv(self):
        """
        Reads a CSV file with detected encoding and a tab delimiter.

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

            # Read the CSV file with pandas using the detected encoding and a tab delimiter
            df = pd.read_csv(self.file_path, sep='\t', encoding=encoding)
            print(f"File '{self.file_path}' read successfully with encoding '{encoding}'")
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
