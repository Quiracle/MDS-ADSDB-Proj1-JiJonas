from os.path import isfile, join, isdir
import duckdb
import os

DB_PATH = "./formatted_zone/formatted.db"
SOURCE = './persistent_landing/'

def getAllFilesRecursive(root):
    root = os.path.normpath(root)
    
    files = [join(root, f) for f in os.listdir(root) if isfile(join(root, f))]
    dirs = [d for d in os.listdir(root) if isdir(join(root, d))]

    for d in dirs:
        files_in_d = getAllFilesRecursive(join(root, d))
        if files_in_d:
            files.extend(files_in_d)

    return files

def load_database():
    files = getAllFilesRecursive(SOURCE)
    with duckdb.connect(DB_PATH) as con:
        con.sql("""
            INSTALL spatial;
            LOAD spatial;
            """)
        for file in files:
            file = file.replace("\\", "/")
            filename = os.path.basename(file).split('.')
            print(file)
            read_function = f'read_{filename[-1]}'
            if filename[-1] == 'xlsx':
                read_function = 'st_read'
            con.sql(f"CREATE TABLE IF NOT EXISTS {'_'.join(filename[0].split('_')[::-1])} AS FROM {read_function}('{file}');")
            if filename[0].split('_')[0].isdigit():
                con.sql(f"ALTER TABLE {'_'.join(filename[0].split('_')[::-1])} ADD COLUMN timestamp VARCHAR DEFAULT '{'-'.join(filename[0].split('_')[:-1])}';")
                print('-'.join(filename[0].split('_')[:-1]))

if __name__ == "__main__":
    load_database()