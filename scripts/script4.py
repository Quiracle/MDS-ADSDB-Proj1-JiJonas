import duckdb
import os


trusted_db_path = './trusted_zone/trusted.db'
exploitation_folder = './exploitation_zone/'

os.makedirs(exploitation_folder, exist_ok=True)

exploitation_db_path = os.path.join(exploitation_folder, 'exploitation.db')

def create_connections():
    # Connect to the existing trusted.db
    trusted_con = duckdb.connect(database=trusted_db_path)

    # Connect to the new exploitation.db
    exploitation_con = duckdb.connect(database=exploitation_db_path)
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

    exploitation_con.executemany("""
    INSERT INTO neighborhood (district, neighborhood) VALUES (?, ?)
    ON CONFLICT DO NOTHING
    """, NEIGHBORHOOD_data)

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

    exploitation_con.executemany("""
    INSERT INTO idealista (propertyCode, price, neighborhood, timestamp) VALUES (?, ?, ?, ?)
    """, idealista_data)

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

    exploitation_con.executemany("""
    INSERT INTO house (
        propertyCode, floor, propertyType, size, status, newDevelopment, hasLift, rooms, 
        bathrooms, exterior, latitude, longitude, address, province, municipality, 
        district, country, neighborhood, priceByArea, timestamp
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, house_data)

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
    trusted_con, exploitation_con = create_connections()

    tables_to_drop = ['house', 'idealista', 'income', 'neighborhood']
    drop_tables(tables_to_drop, exploitation_con)

    create_all_tables(trusted_con, exploitation_con)

    close_connections(trusted_con, exploitation_con)

if __name__ == "__main__":
    trusted_con, exploitation_con = create_connections()

    tables_to_drop = ['house', 'idealista', 'income', 'neighborhood']
    drop_tables(tables_to_drop, exploitation_con)

    create_all_tables(trusted_con, exploitation_con)

    close_connections(trusted_con, exploitation_con)