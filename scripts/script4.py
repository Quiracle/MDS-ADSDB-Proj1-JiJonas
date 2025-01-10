import duckdb
import os


SOURCE_DB = './trusted_zone/trusted.db'
DESTINATION_FOLDER = './exploitation_zone/'

DESTINATION_DB = os.path.join(DESTINATION_FOLDER, 'exploitation.db')

def create_connections(source, destination):
    # Connect to the existing trusted.db
    trusted_con = duckdb.connect(database=source)

    # Connect to the new exploitation.db
    exploitation_con = duckdb.connect(database=destination)
    return trusted_con, exploitation_con

def drop_tables(tables, exploitation_con):
# Drop existing tables to ensure a clean slate

    for table in tables:
        drop_query = f"DROP TABLE IF EXISTS {table};"
        exploitation_con.execute(drop_query)

def create_neighborhood_table(trusted_con, exploitation_con):
    exploitation_con.execute("""
    CREATE OR REPLACE TABLE neighborhood (
        district VARCHAR,
        neighborhood VARCHAR,
        PRIMARY KEY (district, neighborhood)
    );
    """)

    NEIGHBORHOOD_data = trusted_con.execute("""
    SELECT DISTINCT district, neighborhood
    FROM income
    """).fetchall()

    filtered_list = [tup for tup in NEIGHBORHOOD_data if None not in tup]


    exploitation_con.executemany("""
    INSERT INTO neighborhood (district, neighborhood) VALUES (?, ?)
    ON CONFLICT DO NOTHING
    """, filtered_list)

def create_income_table(trusted_con, exploitation_con):
    exploitation_con.execute("""
    CREATE OR REPLACE TABLE income (
        neighborhood VARCHAR,
        rdlpc_eur DOUBLE,
        year VARCHAR,
        PRIMARY KEY (neighborhood, year)
    );
    """)

    income_data = trusted_con.execute("""
    SELECT neighborhood, rdlpc_eur, year
    FROM income
    """).fetchall()

    exploitation_con.executemany("""
    INSERT INTO income (neighborhood, rdlpc_eur, year) VALUES (?, ?, ?)
    ON CONFLICT DO NOTHING
    """, income_data)

def create_idealista_table(trusted_con, exploitation_con):
    exploitation_con.execute("""
    CREATE OR REPLACE TABLE idealista (
        propertyCode VARCHAR,
        price DOUBLE,
        neighborhood VARCHAR,
        timestamp TIMESTAMP_NS,
        PRIMARY KEY (propertyCode, timestamp)
    );
    """)

    idealista_data = trusted_con.execute("""
    SELECT propertyCode, price, neighborhood, timestamp
    FROM idealista
    """).fetchall()

    unique_data = list({(row[0], row[3]): row for row in idealista_data}.values())

    exploitation_con.executemany("""
    INSERT INTO idealista (propertyCode, price, neighborhood, timestamp) VALUES (?, ?, ?, ?)
    """, unique_data)

def create_house_table(trusted_con, exploitation_con):
    exploitation_con.execute("""
    CREATE OR REPLACE TABLE house (
        propertyCode VARCHAR,
        timestamp TIMESTAMP_NS,
        floor VARCHAR,
        propertyType VARCHAR,
        size DOUBLE,
        status VARCHAR,
        newDevelopment BOOLEAN,
        hasLift BOOLEAN,
        rooms INT,
        bathrooms INT,
        exterior BOOLEAN,
        latitude DOUBLE,
        longitude DOUBLE,
        address VARCHAR,
        province VARCHAR,
        municipality VARCHAR,
        district VARCHAR,
        country VARCHAR,
        neighborhood VARCHAR,
        priceByArea DOUBLE,
        PRIMARY KEY (propertyCode, timestamp),
        FOREIGN KEY (propertyCode, timestamp) REFERENCES idealista(propertyCode, timestamp)
    );
    """)

    # Populate 'house' table
    house_data = trusted_con.execute("""
    SELECT 
        propertyCode,
        floor,
        propertyType,
        size,
        status,
        newDevelopment,
        hasLift,
        rooms,
        bathrooms,
        exterior,
        latitude,
        longitude,
        address,
        province,
        municipality,
        district,
        country,
        neighborhood,
        priceByArea,
        timestamp
    FROM idealista
    """).fetchall()

    unique_data = list({(row[0], row[19]): row for row in house_data}.values())

    exploitation_con.executemany("""
    INSERT INTO house (
        propertyCode, floor, propertyType, size, status, newDevelopment, hasLift, rooms, 
        bathrooms, exterior, latitude, longitude, address, province, municipality, 
        district, country, neighborhood, priceByArea, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, unique_data)

def create_all_tables(trusted_con, exploitation_con):
    create_neighborhood_table(trusted_con, exploitation_con)
    create_income_table(trusted_con, exploitation_con)
    create_idealista_table(trusted_con, exploitation_con)
    create_house_table(trusted_con, exploitation_con)

def close_connections(trusted_con, exploitation_con):
    # Connect to the existing trusted.db
    trusted_con.close()

    # Connect to the new exploitation.db
    exploitation_con.close()

def run():
    if not os.path.exists(SOURCE_DB):
        return (f"File not found: {SOURCE_DB}")

    os.makedirs(DESTINATION_FOLDER, exist_ok=True)

    trusted_con, exploitation_con = create_connections(SOURCE_DB, DESTINATION_DB)

    tables_to_drop = ['house', 'idealista', 'income', 'neighborhood']
    drop_tables(tables_to_drop, exploitation_con)

    create_all_tables(trusted_con, exploitation_con)

    close_connections(trusted_con, exploitation_con)

if __name__ == "__main__":
    if not os.path.exists(SOURCE_DB):
        exit 
    os.makedirs(DESTINATION_FOLDER, exist_ok=True)

    trusted_con, exploitation_con = create_connections(SOURCE_DB, DESTINATION_DB)

    tables_to_drop = ['house', 'idealista', 'income', 'neighborhood']
    drop_tables(tables_to_drop, exploitation_con)

    create_all_tables(trusted_con, exploitation_con)

    close_connections(trusted_con, exploitation_con)