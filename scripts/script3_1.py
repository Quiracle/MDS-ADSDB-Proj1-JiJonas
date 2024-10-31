import duckdb
import pandas as pd
import numpy as np
import os
import sweetviz as sv
from customized_profiling import customized_profiling



def data_profiling(db_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    # Get list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    
    # Iterate over each table and load into a DataFrame
    for table in tables:
        table_name = table[0]
        print(f"Reading and profiling table: {table_name}")
        try:
            # Read the table into a DataFrame
            df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Create a Sweetviz report for the DataFrame
            report = sv.analyze(df)
            report_path = os.path.join(output_dir, f"{table_name}_report.html")
            report.show_html(report_path)
        except Exception as e:
            print(f"Error while profiling table '{table_name}': {e}")
    
    # Close the connection
    conn.close()

# data_profiling('../trusted_zone/trusted.db', '../trusted_zone/')



def profile_database(db_path, output_path):
    if not os.path.exists(output_path):
        os.makedirs(output_path)
    
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    # Get list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    # Iterate over each table and load into a DataFrame
    for table in tables:
        table_name = table[0]
        print(f"Reading and profiling table: {table_name}")
        try:
            # Read the table into a DataFrame
            df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Perform customized profiling
            customized_profiling(df, table_name, output_path)
        except Exception as e:
            print(f"Error while profiling table '{table_name}': {e}")
    
    # Close the connection
    conn.close()


def run():
    profile_database('./trusted_zone/trusted.db', './plots/trusted_profiling')

if __name__ == "__main__":
    profile_database('./trusted_zone/trusted.db', './plots/trusted_profiling')


