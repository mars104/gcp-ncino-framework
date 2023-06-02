# Salesforce-BigQuery Data Comparison Tool

This Python-based tool offers an automated solution to validate and reconcile data between Salesforce and BigQuery systems. The tool reduces the dependency on manual processes of extracting and comparing data, which can be labor-intensive, error-prone, and time-consuming. It performs the task of reading test cases from a CSV file, fetching data from both BigQuery and Salesforce, and then compares the data sets to highlight any differences. By automating the entire process, the tool significantly increases test execution speed, accuracy, and reliability, facilitating a scalable and efficient testing process.

## Key Features
Automated Data Extraction and Comparison: The tool fetches data directly from Salesforce and BigQuery using provided SQL queries, compares them, and highlights the differences.

Nested Dictionary Handling: This tool effectively deals with nested dictionaries in Salesforce data, for example we have the collection obeject LLC_BI__LookupKey__c in eaxmple test TC_LG_001.

Customisable Index Mapping: It allows users to define custom mapping for the index columns between Salesforce and BigQuery systems.

Result Documentation: After executing the comparison, the tool automatically writes the results and the summary of the comparison to a CSV file. This provides  both test evidence and data to facilitate further investations/troubleshooting if required.

Improved Testing Process: By automating the extraction and comparison of data, it significantly improves the testing process' speed, accuracy, and scalability.

## Description

The tool reads test cases from a CSV file, each containing two SQL queries to fetch data from BigQuery and Salesforce. It then processes the data sets to handle nested dictionaries, standardizes the column names, sorts them, and compares the two sets. 

Finally, it records the differences and writes the comparison results to a CSV file. 

## Dependencies

The script uses the following Python libraries:
- `simple_salesforce` for interfacing with Salesforce.
- `google.cloud` for interfacing with Google's BigQuery.
- `pandas` for data handling and manipulation.
- `numpy` for numerical computations.
- `os` for handling file paths.
- `collections` for additional data structure functionalities.

## Configuration

1. **Salesforce Connection:** Replace `'x.x@lloydsbanking.com.devpoc1'` and `'xxxx'` with your Salesforce username and password respectively. Replace `'yourkey'` and `'your secret'` with your consumer key and secret respectively. Replace `'test'` with your domain.

2. **BigQuery Client:** Ensure your Google Cloud credentials are correctly set up in your environment for the BigQuery client to work.

3. **Test Cases:** The script expects a CSV file named `tests.csv` in the same directory. This file should contain all the test cases that need to be executed. The SQL folder is where sql tests scripts can be stored as text files for use in your tests.

## Usage

### Test Cases CSV Format

The `tests.csv` file should have the following columns:
1. `Test Name`: The unique name of the test case.
2. `Bigquery SQL`: The SQL query to fetch data from BigQuery or a filename containing the SQL query.
3. `salesforce SQL`: The SQL query to fetch data from Salesforce or a filename containing the SQL query.
4. `bigquery index`: The index columns for the BigQuery data, enclosed in brackets and separated by commas.
5. `salesforce index`: The index columns for the Salesforce data, enclosed in brackets and separated by commas.
6. `execute`: A binary flag indicating whether the test case should be executed (1) or not (0).

### Running the Script

After setting up the configuration, simply run the script using a Python interpreter:
```bash
python framework.py
```
### Results

After running the script, it will create a directory named `Results` in the same directory. Inside `Results`, it will generate two CSV files for each test case:
1. `TestName_results.csv`: The comparison results of the test case.
2. `TestName_results_summary.csv`: The summary of the comparison, including total rows, matching rows, and non-matching rows.

## Troubleshooting

In case of any errors:
- Verify your Salesforce and Google Cloud credentials.
- Check if your SQL queries are correctly formatted and fetch the right data. For 
- ple running queries in salesfoce developer console or bigquery client.
- Check if your test case CSV file is correctly formatted.
- Verify that the CSV file, SQL files, and Python script are in the correct directories as expected by the script.

## Contribution

Feel free to fork this project, contribute and make a pull request.
