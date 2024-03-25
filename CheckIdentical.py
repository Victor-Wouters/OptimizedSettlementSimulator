import pandas as pd

# file paths
file_path1 = '1.csv'
file_path2 = '2.csv'

def check_csv_identity(file_path1, file_path2):
    # Read the CSV files
    try:
        df1 = pd.read_csv(file_path1)
        df2 = pd.read_csv(file_path2)
    except Exception as e:
        return f"Error reading files: {e}"

    # Check if the shape (row and column counts) are the same
    if df1.shape != df2.shape:
        return "The files have different shapes."

    # Check if all elements are identical
    if not df1.equals(df2):
        return "The files are not identical."

    return "The files are fully identical."

# Run the comparison
result = check_csv_identity(file_path1, file_path2)
print(result)