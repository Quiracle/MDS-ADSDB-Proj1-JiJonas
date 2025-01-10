import duckdb
import math
import os
from difflib import SequenceMatcher

SOURCE_DB = './exploitation_zone/exploitation.db'
DESTINATION_FOLDER = './model_zone/'

DESTINATION_DB = os.path.join(DESTINATION_FOLDER, 'model.db')

os.makedirs("model_zone", exist_ok=True)

def calculate_distance(lat1, lon1, lat2, lon2):
    """Calculate the Haversine distance between two geographic points."""
    R = 6371  # Radius of the Earth in kilometers
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def find_closest_match(name, candidates):
    """Find the closest matching name from a list of candidates."""
    return max(candidates, key=lambda x: SequenceMatcher(None, name, x).ratio())

def create_analytical_sandbox(source_db_path, target_db_path):
    # Connect to the source and target DuckDB databases
    source_conn = duckdb.connect(source_db_path)
    target_conn = duckdb.connect(target_db_path)

    # Fetch unique neighborhoods from the income table
    income_neighborhoods = source_conn.execute("SELECT DISTINCT LOWER(neighborhood) AS neighborhood FROM income").fetchdf()['neighborhood'].tolist()

    # Update house table with closest neighborhood match
    house_data = source_conn.execute("SELECT DISTINCT LOWER(neighborhood) AS neighborhood FROM house WHERE neighborhood IS NOT NULL").fetchdf()
    house_data['closest_match'] = house_data['neighborhood'].apply(lambda x: find_closest_match(x, income_neighborhoods))

    # Update the house table with the matched neighborhoods
    # for _, row in house_data.iterrows():
    #     source_conn.execute(
    #     """
    #     UPDATE house
    #     SET neighborhood = ?
    #     WHERE LOWER(neighborhood) = ?;
    #     """,
    #     (row['closest_match'], row['neighborhood'])
    #     )

    # Query the source database to create the analytical sandbox data
    query = """
    SELECT 
        h.propertyCode, 
        h.size, 
        h.rooms, 
        h.bathrooms, 
        h.latitude, 
        h.longitude, 
        h.neighborhood, 
        i.price, 
        n.rdlpc_eur AS income_level, 
        CAST(i.timestamp AS DATE) AS date,
        CAST(SUBSTRING(CAST(i.timestamp AS VARCHAR), 6, 2) AS INT) AS month,  -- Extract month from timestamp
        CAST(SUBSTRING(CAST(i.timestamp AS VARCHAR), 1, 4) AS INT) AS year,   -- Extract year from timestamp
        6371 * 2 * ASIN(SQRT(POWER(SIN((h.latitude - 41.38879) * PI() / 180 / 2), 2) + COS(h.latitude * PI() / 180) * COS(41.38879 * PI() / 180) * POWER(SIN((h.longitude - 2.15899) * PI() / 180 / 2), 2))) AS distance_to_center
    FROM house h
    INNER JOIN idealista i 
        ON h.propertyCode = i.propertyCode AND h.timestamp = i.timestamp
    LEFT JOIN income n 
        ON LOWER(h.neighborhood) = LOWER(n.neighborhood);
    """

    # Fetch the analytical sandbox data from the source database
    sandbox_data = source_conn.execute(query).fetchall()

    # Define the schema for the target database
    target_conn.execute("""
    CREATE OR REPLACE TABLE analytical_sandbox (
        propertyCode VARCHAR,
        size DOUBLE,
        rooms INT,
        bathrooms INT,
        latitude DOUBLE,
        longitude DOUBLE,
        neighborhood VARCHAR,
        price DOUBLE,
        income_level DOUBLE,
        date DATE,
        month INT,
        year INT,
        distance_to_center DOUBLE
    );
    """)

    # Insert the data into the target database
    target_conn.executemany("""
    INSERT INTO analytical_sandbox (
        propertyCode, size, rooms, bathrooms, latitude, longitude, neighborhood,
        price, income_level, date, month, year, distance_to_center
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, sandbox_data)

    # Print success message
    print("Analytical sandbox table created successfully in the target database.")

    # Disconnect from the databases
    source_conn.close()
    target_conn.close()

def run():
    create_analytical_sandbox(SOURCE_DB, DESTINATION_DB)

if __name__ == "__main__":
    run()