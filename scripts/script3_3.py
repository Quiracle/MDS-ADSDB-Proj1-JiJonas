import duckdb
import pandas as pd
import numpy as np


def consistent_formatting_idealista(df):
    # Standardize null values
    df = df.replace('NULL', np.nan)
    
    # Normalize data types
    df['propertyCode'] = df['propertyCode'].astype(str)
    df['price'] = df['price'].astype(float)
    df['size'] = df['size'].astype(float)
    df['exterior'] = df['exterior'].astype(bool)
    df['rooms'] = pd.to_numeric(df['rooms'], errors='coerce').fillna(0).astype(int)
    df['bathrooms'] = pd.to_numeric(df['bathrooms'], errors='coerce').fillna(0).astype(int)
    df['latitude'] = df['latitude'].astype(float)
    df['longitude'] = df['longitude'].astype(float)
    df['hasLift'] = df['hasLift'].astype(bool)
    df['priceByArea'] = df['priceByArea'].astype(float)
    df['newDevelopment'] = df['newDevelopment'].astype(bool)
    
    # Standardize categorical columns by stripping leading/trailing spaces and making them lowercase
    categorical_columns = ['propertyType', 'operation', 'address', 'province', 'municipality', 
                           'district', 'country', 'neighborhood', 'status', 'floor']
    for col in categorical_columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

    # Handle missing categorical values by replacing them with 'unknown'
    df[categorical_columns] = df[categorical_columns].fillna('unknown')
    
    # Standardize floor values (e.g., convert 'entresuelo', 'bajo' to standard text)
    df['floor'] = df['floor'].replace({
        'bj': 'bajo',
        'ss': 'sotano',
        'en': 'entresuelo'
    })
    
    # Format 'timestamp' column to datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    
    return df



def consistent_formatting_idealista_script(db_path, table_name='idealista'):

    try:
        # Connect to the DuckDB database
        con = duckdb.connect(db_path)
        print(f"Connected to DuckDB database at '{db_path}'.")
        
        # Check if the table exists
        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
        existing_tables = [table[0] for table in tables]
        
        if table_name not in existing_tables:
            print(f"Error: Table '{table_name}' does not exist in the database.")
            con.close()
            return
        
        print(f"Processing table: '{table_name}'.")
        
        # Read the table into a DataFrame and Apply consistent formatting
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf() 
        formatted_df = consistent_formatting_idealista(df)
        
        # Overwrite the original table with the formatted DataFrame
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.register('formatted_df', formatted_df)
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM formatted_df")
        print(f"Table '{table_name}' has been overwritten with formatted data.")
        
        # Close the connection
        con.close()
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")




def consistent_formatting_income(df):
    # Standardize null values
    df = df.replace('NULL', np.nan)
    
    df.rename(columns={
        'Distric': 'district',
        'Barris': 'neighborhood',
        'RDLpc (€)': 'rdlpc_eur',
        'Index (RDLpc)': 'index_rdlpc',
        'RPLpc (€)': 'rplpc_eur',
        'Index (RPLpc)': 'index_rplpc',
        'timestamp': 'year'
    }, inplace=True)

    # Normalize data types
    df['rdlpc_eur'] = df['rdlpc_eur'].astype(float)
    df['rplpc_eur'] = df['rplpc_eur'].astype(float)
    df['index_rdlpc'] = df['index_rdlpc'].astype(float)
    df['index_rplpc'] = df['index_rplpc'].astype(float)
    
    # Standardize categorical columns by stripping leading/trailing spaces and making them lowercase
    categorical_columns = ['district', 'neighborhood']
    for col in categorical_columns:
        df[col] = df[col].astype(str).str.strip().str.lower()

    # Handle missing categorical values by replacing them with 'unknown'
    df[categorical_columns] = df[categorical_columns].fillna('unknown')
    
    # Format 'year' column to datetime
    df['year'] = pd.to_datetime(df['year'], format='%Y', errors='coerce')
    
    return df

def consistent_formatting_income_script(db_path, table_name='income'):

    try:
        # Connect to the DuckDB database
        con = duckdb.connect(db_path)
        print(f"Connected to DuckDB database at '{db_path}'.")
        
        # Check if the table exists
        tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
        existing_tables = [table[0] for table in tables]
        
        if table_name not in existing_tables:
            print(f"Error: Table '{table_name}' does not exist in the database.")
            con.close()
            return
        
        print(f"Processing table: '{table_name}'.")
        
        # Read the table into a DataFrame and Apply consistent formatting
        df = con.execute(f"SELECT * FROM {table_name}").fetchdf() 
        formatted_df = consistent_formatting_income(df)
        
        # Overwrite the original table with the formatted DataFrame
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.register('formatted_df', formatted_df)
        con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM formatted_df")
        print(f"Table '{table_name}' has been overwritten with formatted data.")
        
        # Close the connection
        con.close()
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def run():
    consistent_formatting_idealista_script('./trusted_zone/trusted.db', table_name='idealista')
    consistent_formatting_income_script('./trusted_zone/trusted.db', 'income')


