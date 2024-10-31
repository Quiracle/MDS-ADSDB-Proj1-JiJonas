import duckdb
import pandas as pd
import os


def remove_idealista_outliers(db_path):
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    table_name = "idealista"

    try:
        # Count initial number of records
        initial_count = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        
        # Remove records with size larger than 1000 m²
        conn.execute(f"DELETE FROM {table_name} WHERE size > 1000;")
        
        # Remove records with price higher than 5,000,000
        conn.execute(f"DELETE FROM {table_name} WHERE price > 5000000;")
        
        # Remove records with operation type 'rent'
        conn.execute(f"DELETE FROM {table_name} WHERE operation = 'rent';")
        
        # Remove records where province is not 'Barcelona'
        conn.execute(f"DELETE FROM {table_name} WHERE province != 'barcelona';")
        
        # Remove records with municipality 'other'
        conn.execute(f"DELETE FROM {table_name} WHERE municipality != 'barcelona';")

        # Remove records where country is not 'Spain'
        conn.execute(f"DELETE FROM {table_name} WHERE country != 'es';")
        
        # Count final number of records
        final_count = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        
        # Calculate the number of outliers removed
        removed_outliers = initial_count - final_count
        print(f"Number of outliers removed: {removed_outliers}")
        
    except Exception as e:
        print(f"Error while removing outliers from table '{table_name}': {e}")
    
    # Close the connection
    conn.close()


def remove_income_outliers(db_path):
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    table_name = "income"

    try:
        # Count initial number of records
        initial_count = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]

        conn.execute(f"DELETE FROM {table_name} WHERE district IS NULL;")
        
        # Count final number of records
        final_count = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
        
        # Calculate the number of outliers removed
        removed_outliers = initial_count - final_count
        print(f"Number of outliers removed: {removed_outliers}")
        
    except Exception as e:
        print(f"Error while removing outliers from table '{table_name}': {e}")
    
    # Close the connection
    conn.close()

def run():
    remove_idealista_outliers('./trusted_zone/trusted.db')
    remove_income_outliers('./trusted_zone/trusted.db')

if __name__ == "__main__":
    remove_idealista_outliers('./trusted_zone/trusted.db')
    remove_income_outliers('./trusted_zone/trusted.db')
