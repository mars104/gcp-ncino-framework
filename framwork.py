
from simple_salesforce import Salesforce

from google.cloud import bigquery

import pandas as pd
import os
import numpy as np

# create a BigQuery client
client = bigquery.Client()


sf = Salesforce(username='neil.xxx@lloydsbanking.com.devpoc1', password='****', consumer_key='***', consumer_secret='ED185EBE9BE8184EBC6ED9F69236868B749A86168EE893382FA0AD92480FD8D0', domain='test')


# Define the relative path
relative_path = os.path.dirname(os.path.abspath(__file__))

# Read test cases from CSV
test_cases = pd.read_csv(os.path.join(relative_path, 'tests.csv'))

# For each test case
for _, row in test_cases.iterrows():
    if row['execute'] == 1:
        # Extract the queries and indices from the row
        test_name = row['Test Name']
        bigquery_query = row['Bigquery SQL']
        salesforce_query = row['salesforce SQL']
        bigquery_index = eval(row['bigquery index'])
        salesforce_index = eval(row['salesforce index'])

        # BigQuery
        df_bigquery = client.query(bigquery_query).to_dataframe()

        # Salesforce
        sf_data = sf.query_all(salesforce_query)
        df_salesforce = pd.DataFrame(sf_data['records']).drop(columns='attributes')

        # Standardize column names
        df_bigquery.columns = df_bigquery.columns.str.strip().str.lower()
        df_salesforce.columns = df_salesforce.columns.str.strip().str.lower()

        # Check and convert if the index columns are dict type
        for i, index_field in enumerate(salesforce_index):
            if isinstance(df_salesforce[index_field].iloc[0], dict):
                # Extract the key for the data
                data_key = [key for key in df_salesforce[index_field].iloc[0].keys() if key.endswith('__c')]
                # Check if we found a key ending with '__c'
                if data_key:
                    # Extract the required data
                    extracted_data = df_salesforce[index_field].apply(lambda d: d.get(data_key[0], np.nan))
                    # Rename the extracted data with the parent field name
                    df_salesforce[index_field] = extracted_data

    
        # Set index
        df_bigquery.set_index(bigquery_index ,inplace=True)
        df_salesforce.set_index(salesforce_index ,inplace=True)

        # Sort dataframes
        df_bigquery.sort_index(inplace=True)
        df_salesforce.sort_index(inplace=True)

        

        # Merge the dataframes on the index
        merged_df = pd.merge(df_bigquery, df_salesforce, left_index=True, right_index=True, how='outer', indicator=True)

        # Rename the labels of the '_merge' column
        merged_df['_merge'] = merged_df['_merge'].map({'left_only': 'bigquery_only', 'right_only': 'salesforce_only', 'both': 'both'})

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

