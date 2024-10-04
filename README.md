# **SEMT_py: Semantic Enrichment of Tabular Data**

**Version:** 0.1.1  
**Repository:** [SEMT_py GitHub Repository](https://github.com/unimib-datAI/Semtui-python)

---

## **Overview**

**SemT-py** is a Python library designed for the semantic enrichment of tabular data. It facilitates the transformation, modification, and enhancement of tables with additional semantic information. The package is modular, making it adaptable for both expert users and non-experts, offering an intuitive approach to complex data enrichment tasks.

With this package, users can reconcile values against external sources and extend their tables with external data, ensuring that the enriched datasets are accurate and valuable for downstream analysis.

---

## **SemT_py Python Library Structure**

```
SemT_py
│
├── __init__.py
├── modification_manager.py
├── dataset_manager.py
├── extension_manager.py
├── reconciliation_manager.py
├── token_manager.py
├── utils.py
│
├── setup.py
├── LICENSE
├── README.md
```

## **Explanation of Main Functions:**

1. **Root Directory (`SemT_py`)**: Contains core library files.
   - `__init__.py`: Initializes the package when imported.
   - `data_handler.py`: Manages data input/output and processing.
   - `modification_manager.py`: Handles modification and enrichment of data.
   - `dataset_manager.py`: Manages dataset operations like loading and merging.
   - `extension_manager.py`: Controls the addition of extensions or plugins to expand library functionalities.
   - `main.py`: The primary entry point for the library; orchestrates key tasks.
   - `reconciliation_manager.py`: Manages the reconciliation of data with external sources.
   - `semtui_evals.py`: Provides evaluation tools or metrics to assess data enrichment.
   - `token_manager.py`: Handles authentication tokens for communication with external services.
   - `utils.py`: Contains utility functions that assist the core functionality.

---

## **Key Features**

- **Modular Structure:** Adaptable for various data enrichment workflows.
- **Semantic Enrichment:** Add meaningful semantic context to tabular data.
- **Reconciliation:** Match table data with external sources for verification.
- **Dataset Management:** Efficient handling and modification of large datasets.
- **Extensions:** Seamlessly integrate additional features through extensions.
- **Evaluation:** Assess the quality of enriched datasets.
- **Ease of Use:** Intuitive for both experts and non-experts.

---

### Installing the SemT_py Python Library

- **Create a Python Virtual Environment**
  - Run: 
    ```bash
    python3 -m venv myenv
    ```

- **Activate the Virtual Environment**
  - **For macOS and Linux:**
    ```bash
    source myenv/bin/activate
    ```
  - **For Windows:**
    ```bash
    myenv\Scripts\activate
    ```

- **Install the SemT-py Library**
  - Run: 
    ```bash
    pip install --upgrade git+https://github.com/unimib-datAI/Semtui-python.git
    ```

- **Set Up Jupyter Kernel for the Virtual Environment**
  - While inside your virtual environment, install the `ipykernel` package to ensure Jupyter can use this environment as a kernel:
    ```bash
    pip install ipykernel
    ```
  - Then, add your virtual environment to Jupyter as a new kernel:
    ```bash
    python -m ipykernel install --user --name=myenv --display-name "Python (myenv)"
    ```

- **Download the Sample Notebooks**
  - To access the sample notebooks (`sample_notebook.ipynb`, `SEMTUI_test_Notebook.ipynb`), download them individually from the following GitHub folder:
    - [Sample Notebooks Folder on GitHub](https://github.com/unimib-datAI/Semtui-python/tree/main/sample%20Notebooks)
  - To download a notebook:
    1. Click on the notebook name (e.g., `sample_notebook.ipynb`).
    2. Find the download button at the top-right corner to download the file.
    3. **Save the notebook in the `myenv/Sample Notebooks/` directory**.
    
    - **Optionally Move Downloaded Files Using the Terminal**:
    If you've downloaded the notebooks to the `Downloads` folder, you can move them to the `myenv` directory using the terminal. Here's how:
    
    - For example, to move `sample_table.csv` and `SEMTUI_test_Notebook.ipynb`:
      ```bash
      mv ~/Downloads/sample_table.csv ~/myenv/Sample\ Notebooks/
      mv ~/Downloads/SEMTUI_test_Notebook.ipynb ~/myenv/Sample\ Notebooks/
      ```

    **Suggested Folder Structure:**

    ````
    project-folder/
    │
    ├── myenv/                # Virtual environment folder
    │   ├── Sample Notebooks/  # Folder to store notebooks and data files
    │   │   ├── sample_notebook.ipynb
    │   │   ├── SEMTUI_test_Notebook.ipynb
    │   │   ├── sample_data.csv    # Newly added sample data file
    │   │  
    └── your_script.py         # Any Python scripts you create
    ```

This way, all the necessary files will be accessible from Jupyter, even if its access is restricted to the `myenv` folder.

- **Explore the Sample Notebook**
  - **Launch Jupyter Notebook**:
    - Run: 
      ```bash
      jupyter notebook
      ```
  - **Switch the Kernel to the Virtual Environment**:
    - After opening a notebook, ensure the kernel is set to the correct virtual environment:
      - In the Jupyter notebook interface, click on **Kernel** > **Change Kernel**.
      - Select **Python (myenv)** to use the virtual environment you set up earlier.

  - **Open the Sample Notebooks**:
    - Navigate to the `myenv/Sample Notebooks/` folder and open either:
      - `sample_notebook.ipynb`
      - `SEMTUI_test_Notebook.ipynb`
  
  - **Run and Review**:
    - Execute the cells to see example implementations.

- **Note**
  - Ensure Git is installed on your system since the library is fetched from a GitHub repository.

---

## **Dependencies**

**SemT-py** relies on the following Python libraries:

- **pandas** - for efficient data handling and manipulation.
- **numpy** - for numerical computations.
- **chardet** - for character encoding detection.
- **PyJWT** - for secure token handling and authentication.
- **fake-useragent** - to generate random user agents for web scraping.
- **requests** - for making HTTP requests to external APIs.

All dependencies are automatically installed when using `pip`.

---

## **Usage**

Here’s a quick start guide to using **SemT-py**:

### **Basic Example**

```python
from SemT_py import dataset_manager, data_modifier, reconciliation_manager

# Load a dataset
dataset = dataset_manager.load_dataset('path_to_dataset.csv')

# Modify the dataset by applying enrichment
modified_dataset = data_modifier.modify_data(dataset)

# Reconcile values from an external source
reconciled_data = reconciliation_manager.reconcile_data(modified_dataset)

# Save the enriched and reconciled dataset
dataset_manager.save_dataset(reconciled_data, 'enriched_dataset.csv')
```

## **How It Works**

The **SemT-py** library works by allowing users to load tabular data, modify it with external semantic information, reconcile it with external data sources, and evaluate the final dataset.

### **Workflow:**
1. **Load Data**: Load raw tabular data from a CSV or other supported formats.
2. **Modify Data**: Apply transformations and add semantic information from external sources.
3. **Reconcile Data**: Match and validate table data with authoritative external sources (e.g., APIs).
4. **Evaluate Data**: Ensure the enriched data is accurate and of high quality.
5. **Save Data**: Export the final, enriched dataset for further use or analysis.

---

## **Extending SEMT_py**

**SemT-py** is designed to be modular and extensible. You can add custom functionalities by writing extensions.

---

## **Example Notebooks**

To help you get started, **SemT-py** comes with example Jupyter notebooks that showcase its functionalities. Open and run these notebooks to see how to implement various tasks such as data loading, enrichment, reconciliation, and evaluation.

```bash
jupyter sample Notebooks/sample_notebook.ipynb
```

---

## **Development**

Feel free to contribute to the project by forking the repository and submitting a pull request. 

### **Setting Up for Development**

To install **SemT-py** locally, follow these steps:

1. **Clone the repository**:
   ```bash
   git clone https://github.com/unimib-datAI/Semtui-python.git
   ```

2. **Navigate to the cloned directory**:
   ```bash
   cd Semtui-python
   ```

3. **Create and activate a virtual environment** (optional but recommended):
   - For macOS/Linux:
     ```bash
     python3 -m venv myenv
     source myenv/bin/activate
     ```
   - For Windows:
     ```bash
     python -m venv myenv
     myenv\Scripts\activate
     ```

4. **Install the library**:
   ```bash
   pip install .
   ```
---
---

## **License**

This project is licensed under the **MIT License**. See the [LICENSE](LICENSE) file for more information.

---

## **Contact**

For any questions, suggestions, or feedback, feel free to reach out to:

- [inside.disco.unimib.it](http://inside.disco.unimib.it/)

---


### **Acknowledgments**
- Special thanks to the open-source community for providing essential libraries that power this project.
