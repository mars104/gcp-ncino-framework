#This Python script performs the following operations:

#It reads the test cases from a CSV file.
#It loops through each test case and checks if it needs to be executed.
#For each test case to be executed, it fetches data from both BigQuery and Salesforce using the SQL queries provided in the CSV file.
#It then handles any nested dictionaries in the Salesforce data.
#It checks if any columns in Salesforce data are dictionaries and if so, extracts the relevant data.
#It creates a mapping dictionary for the index columns and renames Salesforce columns to match BigQuery columns.
#It standardizes the column names in both data sets.
#It sorts both data sets based on index columns and resets the index.
#It compares the two data sets and records the differences.
#Finally, it writes the comparison results to a CSV file.

#the headings in the results are based on the column names in the saleforce queries. 


from simple_salesforce import Salesforce

from google.cloud import bigquery

import pandas as pd
import os
import numpy as np
import collections

# create a BigQuery client
client = bigquery.Client()


sf = Salesforce(username='neil.cameron@lloydsbanking.com.devpoc1', password='xxxx$', consumer_key='xxx', consumer_secret='xxx', domain='test')




# Define the relative path
relative_path = os.path.dirname(os.path.abspath(__file__))

# Read test cases from CSV
test_cases = pd.read_csv(os.path.join(relative_path, 'tests3.csv'))

assert not test_cases.empty, "Test cases dataframe is empty. Check your 'tests.csv' file."
assert all(x in test_cases.columns for x in ['Test Name', 'Bigquery SQL', 'salesforce SQL', 'bigquery index', 'salesforce index', 'execute']), "Not all columns found in the dataframe. Check your 'tests.csv' file."

# Recursively flatten dictionary
def flatten(data, prefix=''):
    """Flatten a dictionary with nested dictionaries."""
    result = {}
    for key, value in data.items():
        if isinstance(value, dict):
            result.update(flatten(value, f'{prefix}{key}_'))
        else:
            result[f'{prefix}{key}'] = value
    return result

def handle_nested_dicts(df):
    for column in df.columns:
        if column.startswith('attributes'):   # Ignore attributes fields
            continue
        if isinstance(df[column].iloc[0], dict):
            dict_keys = df[column].iloc[0].keys()
            for key in dict_keys:
                if key == 'attributes':  # Ignore nested attributes fields
                    continue
                df[f'{column}_{key}'] = df[column].apply(lambda x: x[key] if isinstance(x, dict) else np.nan)
            df = df.drop(columns=[column])
    return df

# For each test case
for _, row in test_cases.iterrows():
    if row['execute'] == 1:
        # Extract the queries and indices from the row
        test_name = row['Test Name']
        bigquery_query = row['Bigquery SQL']
        salesforce_query = row['salesforce SQL']
        bigquery_index = [int(index) for index in row['bigquery index'].strip("[]").split(",")]
        salesforce_index = [int(index) for index in row['salesforce index'].strip("[]").split(",")]

        # BigQuery
        
        df_bigquery = client.query(bigquery_query).to_dataframe()

       
        #start
        # Salesforce
        sf_data = sf.query_all(salesforce_query)

        # Apply 'flatten' function to the list of records
        sf_data_records = [flatten(record) for record in sf_data['records']]
        df_salesforce = pd.DataFrame(sf_data_records)

        # Handle nested dictionaries
        df_salesforce = handle_nested_dicts(df_salesforce)

        # Drop columns that contain 'attributes' in their name
        df_salesforce = df_salesforce[df_salesforce.columns.drop(list(df_salesforce.filter(regex='attributes')))]

        if 'attributes' in df_salesforce.columns:
            df_salesforce = df_salesforce.drop(columns='attributes')

        # Standardize column names
        df_bigquery.columns = df_bigquery.columns.str.strip().str.lower()
        df_salesforce.columns = df_salesforce.columns.str.strip().str.lower()

        # Create the index lists after dropping the 'attributes' columns
        bigquery_index = df_bigquery.columns[bigquery_index].tolist()
        salesforce_index = df_salesforce.columns[salesforce_index].tolist()

        # Create a mapping dictionary for the index columns
        index_mapping = dict(zip(salesforce_index, bigquery_index))
        print("Index Mapping: ", index_mapping)

        #end
        print("BigQuery columns: ", df_bigquery.columns)
        print("Salesforce columns: ", df_salesforce.columns)
        # Rename the columns in df_salesforce and df_bigquery based on the index mapping
        
        index_mapping_inverse = {v: k for k, v in index_mapping.items()}
        df_salesforce.rename(columns=index_mapping_inverse, inplace=True)
        df_bigquery.rename(columns=index_mapping_inverse, inplace=True)

        
        
        
        #df_salesforce.rename(columns=index_mapping, inplace=True)
        #df_bigquery.rename(columns=index_mapping, inplace=True)

        print("BigQuery columns: ", df_bigquery.columns)
        print("Salesforce columns: ", df_salesforce.columns)
        

        # Change to lower case
        index_mapping = {k.lower(): v.lower() for k, v in index_mapping.items()}

        # Merge the dataframes on the index
        #merged_df = pd.merge(df_bigquery, df_salesforce, on=list(index_mapping.values()), how='outer', indicator=True)
        merged_df = pd.merge(df_bigquery, df_salesforce, on=list(index_mapping_inverse.values()), how='outer', indicator=True)


        # Rename the labels of the '_merge' column
        merged_df['_merge'] = merged_df['_merge'].map({'left_only': 'bigquery_only', 'right_only': 'salesforce_only', 'both': 'both'})


        #review column names if required to set index
        print(df_salesforce.columns)
        print(df_bigquery.columns)

        print(df_bigquery.head())
        print(df_salesforce.head())

        # Reset index
        df_bigquery.reset_index(drop=True, inplace=True)
        df_salesforce.reset_index(drop=True, inplace=True)

        # Create temporary column names based on Salesforce column names
        temp_cols = [col for col in df_salesforce.columns if col != 'index']

        # Now merge using the temporary column names
        merged_df = pd.merge(df_bigquery, df_salesforce, on=temp_cols, how='outer', indicator=True)


        # Rename the labels of the '_merge' column
        merged_df['_merge'] = merged_df['_merge'].map({'left_only': 'bigquery_only', 'right_only': 'salesforce_only', 'both': 'both'})

        print(merged_df.head())

        # Count number of matching and non-matching rows
        total_rows = merged_df.shape[0]
        matching_rows = merged_df[merged_df['_merge'] == 'both'].shape[0]
        non_matching_rows = total_rows - matching_rows

        # Count number of matching and non-matching fields
        matching_fields = np.sum(merged_df['_merge'] == 'both')
        total_fields = merged_df.size
        non_matching_fields = total_fields - matching_fields

        # Append the summary to the top of the dataframe
        summary_df = pd.DataFrame({
            'Total Rows': [total_rows],
            'Matching Rows': [matching_rows],
            'Non-matching Rows': [non_matching_rows],
            'Total Fields': [total_fields],
            'Matching Fields': [matching_fields],
            'Non-matching Fields': [non_matching_fields]
        })

        # Save the summary dataframe to a CSV file
        results_dir = os.path.join(relative_path, 'Results')
        os.makedirs(results_dir, exist_ok=True)
        summary_file_path = os.path.join(results_dir, f'{test_name}_results.csv')
        summary_df.to_csv(summary_file_path)

        # Append the merged dataframe to the same CSV file
        merged_df.to_csv(summary_file_path, mode='a')

        # If you only want to see the differences, you can filter the rows where '_merge' is not 'both'
        differences_df = merged_df[merged_df['_merge'] != 'both']
        print(differences_df)

        # Check if the DataFrames are equal
        if df_bigquery.index.equals(df_salesforce.index):
            print(f"Test {test_name} Passed: Both BigQuery and Salesforce results match.")
        else:
            print(f"Test {test_name} Failed: BigQuery and Salesforce results do not match.")

        # Compute total and matching rows
        total_rows = len(merged_df)
        matching_rows = len(merged_df[merged_df['_merge'] == 'both'])
        non_matching_rows = total_rows - matching_rows

        # Prepare the summary dataframe
        summary_df = pd.DataFrame({
            'Total Rows': [total_rows],
            'Matching Rows': [matching_rows],
            'Non-matching Rows': [non_matching_rows]
        }, index=[0])

        # Write the summary dataframe to a separate CSV file
        summary_df.to_csv(os.path.join(results_dir, f'{test_name}_results_summary.csv'), index=False)

        # Write the results dataframe to a separate CSV file
        merged_df.to_csv(os.path.join(results_dir, f'{test_name}_results.csv'), index=True)

       
