import pandas as pd
import requests
import psycopg2
from io import BytesIO
from sqlalchemy import create_engine


def download_file_to_memory(url):
    response = requests.get(url, verify=False)  # Disable SSL verification
    response.raise_for_status()  # Check if the request was successful
    return BytesIO(response.content)


def extract_data_from_excel(file_content):
    df = pd.read_excel(file_content, skiprows=7, sheet_name=None)
    return df


def clean_data(df):
    if df is None:
        print("No data to clean")
        return None

    try:
        # Example data cleaning steps:
        df = df.drop_duplicates()  # Remove duplicate rows
        df = df.fillna(" ")  # Fill missing values with 0

        # Convert float columns to percentages
        float_columns = df.select_dtypes(include=['float64'])
        for col in float_columns:
            df[col] = round(df[col] * 100, 2)
            df[col] = df[col].astype(str) + '%'
        print(df)
        return df
    except Exception as e:
        print(f"Error during data cleaning: {e}")
        return None


def main(url):
    # Step 1: Download the Excel file to memory
    file_content = download_file_to_memory(url)
    print("Downloaded the file to memory")

    # Step 2: Extract data from the Excel file
    dfs = extract_data_from_excel(file_content)
    print(f"Extracted data from Excel: {list(dfs.keys())}\n")

    try:
        conn = psycopg2.connect(
            database="mydb", user='postgres', password='0726', host='localhost', port='5432')
        conn.autocommit = True
        cursor = conn.cursor()
        print("Database connected successfully")

        connection_string = "postgresql+psycopg2://postgres:0726@localhost:5432/mydb"
        engine = create_engine(connection_string)
        print("Engine created")

        for sheet_name, df in dfs.items():
            table_name = input(f"Enter table name for sheet '{sheet_name}': ")

            # Clean the data
            df = clean_data(df)
            print(f"Cleaned data for sheet '{sheet_name}'")

            # Write the DataFrame to the database
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            print(f"DataFrame from sheet '{sheet_name}' written to table '{table_name}'")

            # Execute a query to confirm the data was inserted
            sql_query = f"SELECT * FROM {table_name};"
            print(f"Executing query for table '{table_name}'")
            data = pd.read_sql_query(sql_query, engine)
            print(data)

        print("Executing query")
    except psycopg2.Error as e:
        print("Error connecting to PostgresSQL database:", e)
    finally:
        if conn is not None:
            conn.close()
            print("Database connection closed.")


if __name__ == "__main__":
    url = input("Enter the URL of the Excel file: ")
    main(url)
