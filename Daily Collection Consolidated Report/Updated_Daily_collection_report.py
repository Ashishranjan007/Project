# -*- coding: utf-8 -*-
"""
Created on Thu Feb 13 11:50:04 2025

@author: Ashish Ranjan
@Email: ashishranjan5323@gmail.com
"""

import pandas as pd
import os
import glob


folder_path = r"C:\Users\ACFL\Downloads"

# Step 1:
file_patterns = {
    "overdue_details": "OverDueDetails_*.tsv",
    "loan_disbursement": "LoanDisbursementRegister_*.tsv",
    "Collection_DueData_afteradjustment": "CollectionDueDataafteradjustment_*.tsv",
    "Consolidated_GL_Statement_Summary_2050100": "2ConsolidatedGLStatementSummary_*.tsv",
    "Consolidated_GL_Statement_Summary_10101022": "ConsolidatedGLStatementSummary_*.tsv",
    "Collection_Status_Report": "CollectionStatusReport_*.tsv"
}


def get_latest_file(pattern):
    files = glob.glob(os.path.join(folder_path, pattern))
    if not files:
        print(f"No files found for pattern: {pattern}")
        return None
    return max(files, key=os.path.getctime)


def clean_and_convert(df, columns, target_type):
    """
    Converts specified columns in the DataFrame to the target type.
    Handles errors and missing values gracefully.
    """
    for col in columns:
        if target_type == 'float':
            df[col] = pd.to_numeric(df[col], errors='coerce')  # Convert to float, NaN for invalid
        elif target_type == 'int':
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)  # Convert to int
        elif target_type == 'str':
            df[col] = df[col].fillna('').astype(str)  # Convert to string
    return df


# Load CSV files dynamically
overdue_details = pd.read_csv(get_latest_file(file_patterns["overdue_details"]), delimiter='\t')
loan_disbursement = pd.read_csv(get_latest_file(file_patterns["loan_disbursement"]), delimiter='\t')
Collection_DueData_afteradjustment = pd.read_csv(get_latest_file(file_patterns["Collection_DueData_afteradjustment"]), delimiter='\t')
Consolidated_GL_Statement_Summary_2050100 = pd.read_csv(get_latest_file(file_patterns["Consolidated_GL_Statement_Summary_2050100"]), delimiter='\t')
Consolidated_GL_Statement_Summary_10101022 = pd.read_csv(get_latest_file(file_patterns["Consolidated_GL_Statement_Summary_10101022"]), delimiter='\t')
Collection_Status_Report = pd.read_csv(get_latest_file(file_patterns["Collection_Status_Report"]), delimiter='\t')
# GL_Statement_Summary_Nextday_2050100 = pd.read_csv("CollectionDueDataafteradjustment_PAC001666_2312202413153.csv")  # Replace with the actual file


# Step 2: Standardize column names for `Branch Code`
# Map all possible names for `Branch Code` to a common name
rename_map = {
    'Branch Code': 'Branch_Code',
    'BranchID': 'Branch_Code',
    'OurBranchID': 'Branch_Code',
    'Branch_Code': 'Branch_Code',
    'Branch Identifier': 'Branch_Code',
    'Our Branch ID': 'Branch_Code'
}

overdue_details.rename(columns=rename_map, inplace=True)
loan_disbursement.rename(columns=rename_map, inplace=True)
Collection_DueData_afteradjustment.rename(columns=rename_map, inplace=True)
Consolidated_GL_Statement_Summary_2050100.rename(columns=rename_map, inplace=True)
Consolidated_GL_Statement_Summary_10101022.rename(columns=rename_map, inplace=True)
Collection_Status_Report.rename(columns=rename_map, inplace=True)

# Step 3: Clean and ensure correct data types
# Specify columns to clean and their target types
overdue_details = clean_and_convert(overdue_details, ['Branch_Code', 'Principal Balance'], 'float')
loan_disbursement = clean_and_convert(loan_disbursement, ['Disburesment Amount'], 'float')

# Step 4: Process and aggregate data
# Active Loan and Principal Balance
active_loans = overdue_details.groupby('Branch_Code').agg(
    Active_Loan=('Loan Account Number', 'count'),
    Principal_Balance=('Principal Balance', 'sum')
).reset_index()

# Loan Disbursement
disbursements = loan_disbursement.groupby('Branch_Code').agg(
    Loan_Disbursed=('Disburesment Amount', 'count'),
    Amount_Disbursed=('Disburesment Amount', 'sum')
).reset_index()

# Add other aggregations or calculations if needed for the other files
# Crrent Day Due and Current Collection
collection_data = Collection_DueData_afteradjustment.groupby('Branch_Code').agg(
    Current_Day_Due=('Crrent_M Due', 'sum'),
    Current_Collection=('Current Collection', 'sum')
).reset_index()

# GL Summaries 2050100
gl_summary = Consolidated_GL_Statement_Summary_2050100.groupby('Branch_Code').agg(
    Opening_Balance=('OpeningBalance', 'sum'),
    Cash_Collection=('DebitAmount', 'sum'),
    Bank_Deposit=('CreditAmount', 'sum'),
).reset_index()

# GL Summaries 10101020
gl_summary_1 = Consolidated_GL_Statement_Summary_10101022.groupby('Branch_Code').agg(
    Pre_collection=('CreditAmount', 'sum'),
).reset_index()

# GL Summaries Nextday 2050100

# gl_summary_nextday = GL_Statement_Summary_Nextday_2050100.groupby('Branch_Code').agg(
#     Opening_Balance=('OpeningBalance', 'sum')
# ).reset.index()

# Filter the collection status report by the desired Repayment Mode
repayment_mode_filter = "Transfer"  # Replace with your desired mode
Filtered_collection_status = Collection_Status_Report[Collection_Status_Report['Repayment Mode'] == repayment_mode_filter]

# Group by Branch_Code and sum the Collection Amount
collection_status = Filtered_collection_status.groupby('Branch_Code').agg(
    Cashless_Collection=('Collection Amount', 'sum'),
).reset_index()


# Step 5: Merge data
merged_data = active_loans.merge(disbursements, on='Branch_Code', how='outer')
merged_data = merged_data.merge(collection_data, on='Branch_Code', how='outer')
merged_data = merged_data.merge(gl_summary, on='Branch_Code', how='outer')
merged_data = merged_data.merge(gl_summary_1, on='Branch_Code', how='outer')
final_data = merged_data.merge(collection_status, on='Branch_Code', how='outer')

# Step 6: Output File
# print(final_data.head())

# # Step 7: Save to CSV
# final_data.to_csv("Consolidated_Branch_Data.csv", index=False)

# print("Data consolidation complete! Saved to 'Consolidated_Branch_Data.csv'")


df = final_data


# Replace NaN in other numerical columns with 0
columns_to_replace_with_zero = ['Opening_Balance', 'Pre_collection', 'Cash_Collection', 'Cashless_Collection']
df[columns_to_replace_with_zero] = df[columns_to_replace_with_zero].fillna(0)


df['Cash_Collection'] = df['Cash_Collection'].abs()

# Step 3: Perform calculations
df['Total_Collection'] = df['Cash_Collection'] + df['Cashless_Collection']  # Add two columns
df['Closing_Balance'] = df['Opening_Balance'] + df['Cash_Collection'] - df['Bank_Deposit']
# Calculate the collection ratio and convert it to a percentage
df['Average_Collection'] = (df['Current_Collection'] / df['Current_Day_Due'].replace(0, float('nan'))) * 100
# Format the percentage to 2 decimal places with a '%' symbol
df['Average_Collection'] = df['Average_Collection'].map('{:.2f}%'.format)


# Replace NaN in specific columns with '-'
columns_to_replace_with_dash = ['Loan_Disbursed', 'Amount_Disbursed', 'Current_Day_Due', 'Current_Collection']
df[columns_to_replace_with_dash] = df[columns_to_replace_with_dash].fillna('-')

# # Replace NaN in other numerical columns with 0
# columns_to_replace_with_zero = [
#     'Opening_Balance', 'Pre_collection', 'Cash_Collection', 'Cashless_Collection',
#     'Total_Collection'
# ]
# df[columns_to_replace_with_zero] = df[columns_to_replace_with_zero].fillna(0)

# Ensure proper formatting for any remaining calculations
df['Average_Collection'] = df['Average_Collection'].replace('nan%', '-')


# Reorder columns
selected_column_order = [
    'Branch_Code', 'Active_Loan', 'Principal_Balance', 'Opening_Balance', 'Loan_Disbursed',
    'Amount_Disbursed', 'Current_Day_Due', 'Current_Collection', 'Average_Collection',
    'Pre_collection', 'Cash_Collection', 'Cashless_Collection', 'Total_Collection',
    'Bank_Deposit', 'Closing_Balance']  # Replace with your desired column order
df = df[selected_column_order]

# Drop rows
rows_to_drop = df[df['Branch_Code'].isin([89, 92])].index
df = df.drop(rows_to_drop)

print(df.head(10))


# Step 4: Save the final DataFrame
output_file = r"C:\Users\ACFL\Downloads\Consolidated_Branch_Data_26-05-25.csv"
df.to_csv(output_file, index=False, encoding='utf-8')


print(f"✅ Final file with sorted columns and calculations. Output saved to {output_file}")
