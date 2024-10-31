import duckdb

EXPLOITATION_DB = './exploitation_zone/exploitation.db'

def analyze_idealista_kpis(db_path):
    # Connect to the DuckDB database
    con = duckdb.connect(db_path)
    print(f"Connected to DuckDB database at '{db_path}'.")
    
    # Check if the table exists
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    existing_tables = [table[0] for table in tables]
    
    table_name = 'idealista'
    if table_name not in existing_tables:
        print(f"Error: Table '{table_name}' does not exist in the database.")
        con.close()
        return
    
    print(f"Analyzing KPIs for table: '{table_name}'.")
    
    # KPI 1: Average Property Price
    avg_price_query = f"""
        SELECT ROUND(AVG(price), 2) AS average_price
        FROM {table_name}
    """
    avg_price = con.execute(avg_price_query).fetchone()[0]
    print(f"Average Property Price: €{avg_price}")
    
    # KPI 2: Median Property Price
    median_price_query = f"""
        SELECT ROUND(MEDIAN(price), 2) AS median_price
        FROM {table_name}
    """
    median_price = con.execute(median_price_query).fetchone()[0]
    print(f"Median Property Price: €{median_price}")
    
    # KPI 3: Average Price by Neighborhood
    avg_price_neighborhood_query = f"""
        SELECT neighborhood, ROUND(AVG(price), 2) AS average_price
        FROM {table_name}
        GROUP BY neighborhood
        ORDER BY average_price DESC
    """
    avg_price_neighborhood = con.execute(avg_price_neighborhood_query).fetchdf()
    print("\nAverage Property Price by Neighborhood:")
    print(avg_price_neighborhood)
    
    # KPI 4: Price Trends Over Time
    price_trend_query = f"""
        SELECT date_trunc('month', timestamp) AS month, ROUND(AVG(price), 2) AS average_price
        FROM {table_name}
        GROUP BY month
        ORDER BY month
    """
    price_trend = con.execute(price_trend_query).fetchdf()
    print("\nPrice Trends Over Time:")
    print(price_trend)
    
    # Close the connection
    con.close()
    print("Analysis complete.")

def analyze_house_kpis(db_path):
    # Connect to the DuckDB database
    con = duckdb.connect(db_path)
    print(f"Connected to DuckDB database at '{db_path}'.")
    
    # Check if the table exists
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    existing_tables = [table[0] for table in tables]
    
    table_name = 'house'
    if table_name not in existing_tables:
        print(f"Error: Table '{table_name}' does not exist in the database.")
        con.close()
        return
    
    print(f"Analyzing KPIs for table: '{table_name}'.")
    
    # KPI 1: Average Floor (excluding non-numeric values)
    avg_floor_query = f"""
        SELECT ROUND(AVG(CAST(floor AS DOUBLE)), 2) AS average_floor
        FROM {table_name}
        WHERE floor ~ '^[0-9]+$'
    """
    avg_floor = con.execute(avg_floor_query).fetchone()[0]
    print(f"Average Floor (numeric only): {avg_floor}")
    
    # KPI 2: Average Size of Properties
    avg_size_query = f"""
        SELECT ROUND(AVG(size), 2) AS average_size
        FROM {table_name}
    """
    avg_size = con.execute(avg_size_query).fetchone()[0]
    print(f"Average Property Size: {avg_size} m²")

     # KPI 3: Average Price per Area
    avg_price_by_area_query = f"""
        SELECT ROUND(AVG(priceByArea), 2) AS average_price_by_area
        FROM {table_name}
    """
    avg_price_by_area = con.execute(avg_price_by_area_query).fetchone()[0]
    print(f"Average Price per Area: €{avg_price_by_area} per m²")

    # Close the connection
    con.close()
    print("Analysis complete.")

def analyze_income_kpis(db_path):
    # Connect to the DuckDB database
    con = duckdb.connect(db_path)
    print(f"Connected to DuckDB database at '{db_path}'.")
    
    # Check if the table exists
    tables = con.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'main';").fetchall()
    existing_tables = [table[0] for table in tables]
    
    table_name = 'income'
    if table_name not in existing_tables:
        print(f"Error: Table '{table_name}' does not exist in the database.")
        con.close()
        return
    
    print(f"Analyzing KPIs for table: '{table_name}'.")
    
    # KPI 1: Average RDLpc by Neighborhood
    avg_rdlpc_query = f"""
        SELECT neighborhood, ROUND(AVG(rdlpc_eur), 3) AS average_rdlpc
        FROM {table_name}
        GROUP BY neighborhood
        ORDER BY average_rdlpc DESC
    """
    avg_rdlpc = con.execute(avg_rdlpc_query).fetchdf()
    print("\nAverage RDLpc by Neighborhood:")
    print(avg_rdlpc)
    
    # Close the connection
    con.close()
    print("Analysis complete.")


def run():
    analyze_house_kpis(EXPLOITATION_DB)
    analyze_idealista_kpis(EXPLOITATION_DB)
    analyze_income_kpis(EXPLOITATION_DB)

if __name__ == "__main__":
    run()

