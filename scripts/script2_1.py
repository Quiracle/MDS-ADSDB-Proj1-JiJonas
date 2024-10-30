import duckdb
import os
import re
from datetime import datetime
import pandas as pd
from customized_profiling import customized_profiling

DB_PATH = './formatted_zone/formatted.db'


def create_connection(path):
    return duckdb.connect(path)


def data_profiling(db_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    try:
        # Connect to the DuckDB database
        conn = duckdb.connect(db_path)
    except FileNotFoundError:
        print("Database folder does not exist")
        return
    # Get list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    
    # Extract the most recent table based on the date in the table name
    latest_table = None
    latest_date = None
    date_pattern = re.compile(r'_(\d{2})_(\d{2})_(\d{4})$')
    
    for table in tables:
        table_name = table[0]
        match = date_pattern.search(table_name)
        if match:
            day, month, year = match.groups()
            table_date = datetime.strptime(f"{day}/{month}/{year}", "%d/%m/%Y")
            if latest_date is None or table_date > latest_date:
                latest_date = table_date
                latest_table = table_name
    
    # Profile the latest table if found
    if latest_table:
        print(f"Reading and profiling table: {latest_table}")
        try:
            # Read the table into a DataFrame
            df = conn.execute(f"SELECT * FROM {latest_table}").fetchdf()
            
            # Remove unwanted columns if they exist
            keyword_list = [
                'thumbnail','externalReference','numPhotos','showAddress',
                'url','distance','hasVideo','detailedType','suggestedTexts',
                'hasPlan','has3DTour','has360','hasStaging','topNewDevelopment',
                'parkingSpace','json','index','priceInfo','description',
                'topPlus','highlight','newDevelopmentFinished'
            ]
            df = df.drop(columns=[col for col in keyword_list if col in df.columns], errors='ignore')
            
            # Convert unhashable types (e.g., dict) to string
            df = df.apply(lambda col: col.map(lambda x: str(x) if isinstance(x, dict) else x))
            
            # Perform customized profiling
            customized_profiling(df, latest_table, output_dir)
        except Exception as e:
            print(f"Error while profiling table '{latest_table}': {e}")
    else:
        print("No tables with a valid date format found in the database.")
    
    # Close the connection
    conn.close()

def run():
    data_profiling(DB_PATH, './plots/formatted_profiling')



