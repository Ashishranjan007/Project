# Unolo Dataset
# According to the od report


import pandas as pd

# Load the data
input_file = r"E:\Unolo Data\OverDueDetails_PAC001666_24052025102124.tsv"  # upload latest overdue report in tsv format
mobile_file = r"C:\Users\ACFL\Downloads\ActiveclientData_PAC001666_24052025104124.tsv"  # upload latest activeclient in
employee_file = r"E:\Unolo Data\Employee.csv"  # latest employee data from unolo

address_file = r"E:\Unolo Data\All Branch centre Address.xlsx"  # updated address of centre

input_file_2 = r"E:\Unolo Data\Unolo_uploaded_data\Bucketwise\OD_data_number_23-05-25.xlsx"  # Correcting variable name
output_file = r"E:\Unolo Data\Unolo_uploaded_data\Bucketwise\OD_data_number_23-05-25.xlsx"  # change the file name
output_file_name = r"E:\Unolo Data\Unolo_uploaded_data\OD_data_split_23-05-25.xlsx"
Client_Team = r"E:\Unolo Data\client_team_mapping.csv"


# Read datasets
df_main = pd.read_csv(input_file, delimiter="\t")
df_mobile = pd.read_csv(mobile_file, delimiter="\t")
df_employee = pd.read_csv(employee_file)
df_address = pd.read_excel(address_file)

# Standardize column formats (convert to string, remove spaces, and ensure uppercase where needed)
df_main["Loan Account Number"] = df_main["Loan Account Number"].astype(str).str.strip()
df_mobile["AccountID"] = df_mobile["AccountID"].astype(str).str.strip()
df_employee["Internal Employee ID"] = df_employee["Internal Employee ID"].astype(str).str.strip().str.upper()
df_main["GM"] = df_main["GM"].astype(str).str.strip().str.upper()

# Merge mobile numbers (Ensure we use df_merged_mobile moving forward)
df_merged_mobile = df_main.merge(df_mobile[["AccountID", "Mobile"]],
                                 left_on="Loan Account Number",
                                 right_on="AccountID",
                                 how="left")

df_merged_mobile.drop(columns=["AccountID"], inplace=True)

# Merge Center Address
df_merged_address = df_merged_mobile.merge(df_address[["Center ID", "Center Address"]],
                                           left_on="Group ID",
                                           right_on="Center ID",
                                           how="left")

df_merged_address.drop(columns=["Center ID"], inplace=True)

# Merge employee names
df_merged = df_merged_address.merge(df_employee[["Internal Employee ID", "Employee Name"]],
                                    left_on="GM",
                                    right_on="Internal Employee ID",
                                    how="left")

df_merged.drop(columns=["Internal Employee ID"], inplace=True)

# Debugging: Check for missing Employee Names
missing_gm = df_merged[df_merged["Employee Name"].isna()]
print("⚠️ Missing Employee Names for GM values:\n", missing_gm["GM"].unique())

# Ensure Overdue Amount is numeric
df_merged["Overdue Amount"] = pd.to_numeric(df_merged["Overdue Amount"], errors="coerce").fillna(0)
df_merged["Over Due Days"] = pd.to_numeric(df_merged["Over Due Days"], errors="coerce").fillna(0)

# Convert the "Overdue Date" column to datetime
df_merged["Overdue Date"] = pd.to_datetime(df_merged["Overdue Date"], format="%d/%m/%Y")
# Format the datetime column to "DD-MM-YYYY"
df_merged["Overdue Date"] = df_merged["Overdue Date"].dt.strftime("%d-%m-%Y")


def modify_client_id(client_id):
    client_id = str(client_id)  # Convert to string
    if len(client_id) == 9:
        return "000" + client_id  # Add 3 zeros for length 9
    elif len(client_id) == 10:
        return "00" + client_id  # Add 2 zeros for length 10
    elif len(client_id) == 12:
        return client_id  # No change for length 12
    else:
        return client_id  # Leave as-is for other lengths


# Apply the function to the "Client ID" column in the main DataFrame
df_merged["Client ID"] = df_merged["Client ID"].apply(modify_client_id)

# Add a new column with the value * for each row
df_merged["Country Code"] = 91
df_merged["Can exec change location"] = "Yes"
df_merged["Latitude"] = ""
df_merged["Longitude"] = ""
df_merged["Radius(m)"] = ""

# Replace null/empty values in "Center Address" with "Null"
df_merged["Center Address"] = df_merged["Center Address"].fillna("Null").replace("", "Null")

# Filter records where Overdue Amount is greater than 0
od_data = df_merged[df_merged["Overdue Amount"] > 0]

# Define overdue day ranges
ranges = {
    "0-30 days": (0, 30),
    "31-60 days": (31, 60),
    "61-89 days": (61, 89),
    "90-148 days": (90, 148),
    "149-179 days": (149, 179),
    "180+ days": (180, float("inf"))
}

# ✅ Enable Dynamic Column Selection
selected_columns = [
    "Client ID", "Employee Name", "Loan Account Name", "Country Code", "Mobile",
    "Center Address", "Can exec change location",
    "Latitude", "Longitude", "Radius(m)",
    "Group Name", "Group ID", "Loan Account Number",
    "Overdue Amount", "Overdue Date", "Over Due Days"  # Added Mobile to make sure it appears
]

# Create a dictionary to store filtered data
filtered_data = {
    "OD Report": df_merged[selected_columns],
    "OD Data": od_data[selected_columns]
}

# Apply filtering for different overdue day ranges
for sheet_name, (min_days, max_days) in ranges.items():
    filtered_data[sheet_name] = od_data[(od_data["Over Due Days"] >= min_days) & (od_data["Over Due Days"] <= max_days)][selected_columns]

# Function to split DataFrame into chunks of 5000 rows


def split_dataframe(df, chunk_size=10000):
    return [df[i:i + chunk_size] for i in range(0, df.shape[0], chunk_size)]


# Save to an Excel file with multiple sheets
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for sheet, data in filtered_data.items():
        if not data.empty:
            if sheet in ["OD Report", "OD Data", "0-30 days", "31-60 days", "61-89 days", "90-148 days", "149-179 days", "180+ days" ]:
                data.to_excel(writer, sheet_name=sheet, index=False)
            else:
                if len(data) > 10000:
                    chunks = split_dataframe(data)  # Split the DataFrame into chunks of 5000 rows
                    for i, chunk in enumerate(chunks):
                        chunk.to_excel(writer, sheet_name=f"{sheet}_{i+1}", index=False)  # Append _1, _2, etc., to the sheet name
                else:
                    data.to_excel(writer, sheet_name=sheet, index=False)  # Save the entire DataFrame if it has 5000 rows or fewer

print(f"✅ Data processing complete. Output saved to {output_file}")


# Splited Unolo Dataset
# OD Data splited by 5000 row per sheet
# Read the input Excel file

output_data = pd.read_excel(input_file_2, sheet_name="OD Data", engine="openpyxl")
df_team = pd.read_csv(employee_file)  # sheet_name="Sheet1", engine="openpyxl")

# Load mapping file with Client_Prefix as string and pad with zeros
client_mapping = pd.read_csv(Client_Team, dtype={'Client_Prefix': str})
client_mapping['Client_Prefix'] = client_mapping['Client_Prefix'].str.zfill(4)

# Convert to dictionary with string keys
client_mapping_dict = dict(zip(
    client_mapping['Client_Prefix'].astype(str),
    client_mapping['Team_Name'].astype(str)
))


def modify_client_id(client_id):
    client_id = str(client_id)  # Convert to string
    if len(client_id) == 9:
        return "000" + client_id  # Add 3 zeros for length 9
    elif len(client_id) == 10:
        return "00" + client_id  # Add 2 zeros for length 10
    elif len(client_id) == 12:
        return client_id  # No change for length 12
    else:
        return client_id.zfill(12) if client_id.isdigit() else client_id  # Leave as-is for other lengths


# Apply the function to the "Client ID" column in the main DataFrame
output_data["Client ID"] = output_data["Client ID"].apply(modify_client_id)

'''# Update 'Visible To (*)' column based on conditions
output_data.loc[
    (output_data['Client ID'].astype(str).str.startswith("0091")) &  # Condition 1: Client ID starts with 0091
    (output_data['Employee Name'].isnull() | (output_data['Employee Name'] == "")),  # Condition 2: Visible To (*) is null or blank
    'Employee Name'] = "Team: 0091-Harriya"  # New value to assign'''

# Enhanced blank detection
blank_mask = (
    output_data['Employee Name'].isna() |
    output_data['Employee Name'].astype(str).str.strip().eq('')
)

# Apply mappings with selective progress feedback
replacements_made = False
for prefix, team_name in client_mapping_dict.items():
    prefix = str(prefix).strip()
    prefix_mask = output_data['Client ID'].str.startswith(prefix)
    matches = blank_mask & prefix_mask
    match_count = matches.sum()

    if match_count > 0:
        output_data.loc[matches, 'Employee Name'] = team_name
        print(f"Applied {prefix}: {match_count} replacements")
        replacements_made = True

if not replacements_made:
    print("No replacements were made - check your mapping prefixes against the Client IDs")


# Replace numerical values in 'Center Address' with a name
output_data.loc[
    output_data['Center Address'].astype(str).str.isnumeric(),  # Check if the value is numerical
    'Center Address'] = "Unknown"  # Replace numerical values with "Unknown"

# Define the maximum number of rows per sheet
max_rows_per_sheet = 10000

# Calculate the number of sheets needed
num_sheets = (len(output_data) // max_rows_per_sheet) + 1

# Create a Pandas Excel writer object
with pd.ExcelWriter(output_file_name, engine="openpyxl") as writer:
    for i in range(num_sheets):
        # Calculate the start and end row for the current chunk
        start_row = i * max_rows_per_sheet
        end_row = start_row + max_rows_per_sheet

        # Get the current chunk of data
        chunk = output_data.iloc[start_row:end_row]

        # Define the sheet name
        sheet_name = f"OD Data_{i+1}" if i > 0 else "OD Data"

        # Write the chunk to the sheet
        chunk.to_excel(writer, sheet_name=sheet_name, index=False)

print(f"✅ Data spliting complete. Output saved to {output_file_name}")
