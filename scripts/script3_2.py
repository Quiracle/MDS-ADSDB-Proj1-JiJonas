import duckdb
import pandas as pd
import os


def deduplication(db_path):
    # Connect to the DuckDB database
    conn = duckdb.connect(db_path)
    
    # Get list of all tables in the database
    tables = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    
    # Iterate over each table and deduplicate
    for table in tables:
        table_name = table[0]
        print(f"Reading and deduplicating table: {table_name}")
        try:
            # Count rows before deduplication
            count_before = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
            
            # Deduplication: Remove duplicate rows
            conn.execute(f"CREATE TABLE {table_name}_deduplicated AS SELECT DISTINCT * FROM {table_name};")
            conn.execute(f"DROP TABLE {table_name};")
            conn.execute(f"ALTER TABLE {table_name}_deduplicated RENAME TO {table_name};")
            
            # Count rows after deduplication
            count_after = conn.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()[0]
            
            # Calculate and inform the number of rows removed
            rows_removed = count_before - count_after
            print(f"Deduplicated table '{table_name}': removed {rows_removed} rows.\n")
            
        except Exception as e:
            print(f"Error while deduplicating table '{table_name}': {e}\n")
    
    # Close the connection
    conn.close()

def run():
    deduplication('./trusted_zone/trusted.db')

if __name__ == "__main__":
    deduplication('./trusted_zone/trusted.db')

