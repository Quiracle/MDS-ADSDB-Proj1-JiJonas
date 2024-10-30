import duckdb
import pandas as pd
import numpy as np
import os

SOURCE_FOLDER = "./formatted_zone"
DESTINATION_FOLDER = "./trusted_zone"

SOURCE_DB = f"{SOURCE_FOLDER}/formatted.db"

DESTINATION_DB = f"{DESTINATION_FOLDER}/trusted.db"

def merge_tables_by_keyword(db_path, keyword):
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    # Query to get all table names containing the specified keyword
    query = f"SELECT table_name FROM information_schema.tables WHERE table_name ILIKE '%{keyword}%';"
    table_names = conn.execute(query).fetchall()

    # Extract table names from query result
    table_names = [name[0] for name in table_names]

    # Read each table into a DataFrame and merge them
    merged_df = pd.DataFrame()
    for table_name in table_names:
        try:
            df = conn.execute(f'SELECT * FROM {table_name}').fetchdf()
            merged_df = pd.concat([merged_df, df], ignore_index=True)
        except Exception as e:
            print(f"Error while processing table '{table_name}': {e}")

    # Close the connection
    conn.close()
    
    return merged_df

def save_merged_table_to_new_db(merged_df, new_db_path, table_name):
    # Connect to the new DuckDB database
    conn = duckdb.connect(new_db_path)
    
    # Drop the table if it exists
    conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    
    # Replace problematic objects with NaN to avoid type conversion issues
    merged_df = merged_df.applymap(lambda x: np.nan if isinstance(x, dict) else x)
    
    # Save the merged DataFrame to the new database
    conn.register('merged_df', merged_df)
    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM merged_df")
    
    # Close the connection
    conn.close()

def merge_and_save_all_groups(db_path, new_db_path):
    # Define the keywords to group tables by
    keywords = ['idealista', 'income']
    
    for keyword in keywords:
        # Merge tables by keyword
        merged_df = merge_tables_by_keyword(db_path, keyword)
        
        # Save the merged DataFrame to the new database
        save_merged_table_to_new_db(merged_df, new_db_path, table_name=keyword)




def drop_unwanted_columns(db_path, table_name):
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    # List of columns to drop
    keyword_list = [
        'thumbnail','externalReference','numPhotos','showAddress',
        'url','distance','hasVideo','detailedType','suggestedTexts',
        'hasPlan','has3DTour','has360','hasStaging','topNewDevelopment',
        'parkingSpace','json','index','priceInfo','description',
        'topPlus','highlight','newDevelopmentFinished'
    ]
    
    # Iterate over the keyword list and drop each column if it exists
    for keyword in keyword_list:
        try:
            conn.execute(f"ALTER TABLE {table_name} DROP COLUMN IF EXISTS {keyword}")
        except Exception as e:
            print(f"Error while dropping column '{keyword}': {e}")
    
    # Save the modified table to a new DataFrame
    modified_df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
    conn.close()
    
    for col in modified_df.select_dtypes(include=['object']).columns:
        modified_df[col] = [np.nan if isinstance(x, dict) else x for x in modified_df[col]]
    
    # Connect to the output database and save the modified table
    output_conn = duckdb.connect(db_path)
    output_conn.register('modified_df', modified_df)

    # Drop the table if it exists
    output_conn.execute(f"DROP TABLE IF EXISTS {table_name}")
    output_conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM modified_df")
    output_conn.close()

def run():
    if not os.path.exists(SOURCE_DB):
        return (f"File not found: {SOURCE_DB}")
    if not os.path.exists(DESTINATION_FOLDER):
        os.makedirs(DESTINATION_FOLDER)  # Create the destination folder if it doesn't exist

    merge_and_save_all_groups(SOURCE_DB, DESTINATION_DB)
    drop_unwanted_columns(DESTINATION_DB, 'idealista')

