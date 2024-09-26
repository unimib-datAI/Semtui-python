from data_manager import DataManager
from dataset_manager import DatasetManager
from reconciliation_manager import ReconciliationManager
from utils import Utility
from extension_manager import ExtensionManager

# Usage Example
api_url = "http://localhost:3003/api/"
username = "your_username"
password = "your_password"
data_manager = DataManager(api_url, username, password)

# Obtain auth token
token = data_manager.obtain_auth_token()
print("Token:", token)

# Process CSV data
csv_file_path = "path/to/your/file.csv"
csv_data = data_manager.process_csv_data(csv_file_path)
print(csv_data)