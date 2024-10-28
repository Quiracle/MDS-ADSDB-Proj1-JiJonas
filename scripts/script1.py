import os
from os import listdir
from os.path import isfile, join, isdir
import shutil

SOURCE = "temporal_landing"
DESTINATION = "persistent_landing"

SOURCE_FOLDER = f"./{SOURCE}/"

DESTINATION_FOLDER = f"./{DESTINATION}/"

def getAllFilesRecursive(root):
    files = [ join(root,f) for f in listdir(root) if isfile(join(root,f))]
    dirs = [ d for d in listdir(root) if isdir(join(root,d))]
    for d in dirs:
        files_in_d = getAllFilesRecursive(join(root,d))
        if files_in_d:
            for f in files_in_d:
                files.append(join(root,f))
    return files


def get_destination_folder(filename: str) -> str:
    filename = filename.split('.')[0]
    if 'extended' in filename.lower():
        return f"{filename.split('_')[0]}/"
    return f"{filename.split('_')[-1]}/{filename.split('_')[0]}/"



def copy_new_files(source_folder, destination_folder):
    # Ensure the source and destination folders exist
    if not os.path.exists(source_folder):
        print(f"Source folder '{source_folder}' does not exist.")
        return
    if not os.path.exists(destination_folder):
        os.makedirs(destination_folder)  # Create the destination folder if it doesn't exist

    # Get the list of files in both source and destination folders
    source_files = getAllFilesRecursive(source_folder)
    dest_files = getAllFilesRecursive(destination_folder)

    num_files = 0

    # Copy files that are not already in the destination folder
    for source_file_path in source_files:
        filename = os.path.basename(source_file_path)

        subfolder = get_destination_folder(filename)
        
        destination_subfolder = join(destination_folder + subfolder)
        if not os.path.exists(destination_subfolder):
            os.makedirs(destination_subfolder)

        dest_file_path = os.path.join(destination_subfolder, filename)
        # Only copy if the file does not exist in the destination folder
        if not os.path.isfile(dest_file_path):
            print(f"Copying {filename} to {destination_subfolder}")
            shutil.copy2(source_file_path, dest_file_path)
            num_files += 1
        else:
            print(f"Skipping {filename}, already exists in {destination_folder}")
    return num_files

def extract_year_from_filename(filename):
    # Split the filename by underscores and take the first part
    year = filename.split('_')[0]
    datasource = filename.split('_')[-1].split('.')[0]
    
    # Return the extracted year as an integer
    return int(year), datasource

def run():
    files = copy_new_files(SOURCE_FOLDER, DESTINATION_FOLDER)

    return f"Moved {files} to persistent landing zone"

def reset():
    try:
        shutil.rmtree(DESTINATION_FOLDER)
        return f"removed {DESTINATION_FOLDER} and all its contents"
    except:
        return "Folder already deleted"

if __name__ == "__main__":
    files = copy_new_files(SOURCE_FOLDER, DESTINATION_FOLDER)

