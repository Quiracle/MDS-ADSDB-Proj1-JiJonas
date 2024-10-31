from os.path import isfile, join, isdir
import duckdb
import os

DB_FOLDER = "./formatted_zone"
DB_PATH = f"{DB_FOLDER}/formatted.db"
SOURCE = './persistent_landing/'

def getAllFilesRecursive(root):
    try:
        root = os.path.normpath(root)
        
        files = [join(root, f) for f in os.listdir(root) if isfile(join(root, f))]
        dirs = [d for d in os.listdir(root) if isdir(join(root, d))]

        for d in dirs:
            files_in_d = getAllFilesRecursive(join(root, d))
            if files_in_d:
                files.extend(files_in_d)

        return files
    except FileNotFoundError:
        raise FileNotFoundError

def load_database():
    try:
        files = getAllFilesRecursive(SOURCE)
        with duckdb.connect(DB_PATH) as con:
            con.sql("""
                INSTALL spatial;
                LOAD spatial;
                """)
            for file in files:
                file = file.replace("\\", "/")
                filename = os.path.basename(file).split('.')
                read_function = f'read_{filename[-1]}'
                if filename[-1] == 'xlsx':
                    read_function = 'st_read'
                table_name = '_'.join(filename[0].split('_')[::-1])
                con.execute(f"DROP TABLE IF EXISTS {table_name}")
                con.sql(f"CREATE TABLE IF NOT EXISTS {table_name} AS FROM {read_function}('{file}');")
                print(f"Created table {table_name}")
                if filename[0].split('_')[0].isdigit():
                    con.sql(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS timestamp VARCHAR DEFAULT '{'-'.join(filename[0].split('_')[:-1])}';")
        return "Formatting was executed correctly"
    except FileNotFoundError:
        return f"Source path {SOURCE} not found"
def run():
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)  # Create the destination folder if it doesn't exist

    return load_database()

if __name__ == "__main__":
    if not os.path.exists(DB_FOLDER):
        os.makedirs(DB_FOLDER)  # Create the destination folder if it doesn't exist
    load_database()