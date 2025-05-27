import pandas as pd

# Load the data
output_file = r"E:\Unolo Data\Unolo_deleted_Data\23-05-25.xlsx"
file_path = r"C:\Users\ACFL\Downloads\ClientList (9).xlsx"  # Update with your file path
df = pd.read_excel(file_path, sheet_name="Sheet1")  # Create Sheet1 from the Client Sheet

# Convert 'OD Date' to datetime format, handling errors
df['OD Date'] = pd.to_datetime(df['OD Date'], errors='coerce')

# Filter rows where 'Client Name (*)' starts with '00'
df_filtered = df[df['Client Name (*)'].astype(str).str.startswith("00")]

# Drop the specified columns
columns_to_drop = ['Otp Verified', 'Created By', 'Created At', 'Last Modified At']
df_filtered = df_filtered.drop(columns=columns_to_drop)

# Assign the Value for the (*) columns
df_filtered['To Delete'] = 1

# Split into chunks of 5000 rows max
chunk_size = 10000
total_chunks = (len(df_filtered) // chunk_size) + (1 if len(df_filtered) % chunk_size != 0 else 0)

# Save to a new Excel file with multiple sheets
with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
    for i in range(total_chunks):
        start_row = i * chunk_size
        end_row = start_row + chunk_size
        df_filtered.iloc[start_row:end_row].to_excel(writer, sheet_name=f"Sheet_{i+1}", index=False)

print(f"✅ Filtered data saved to {output_file}")
