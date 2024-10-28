import streamlit as st
import io
from contextlib import redirect_stdout
import os
import script0, script1, script2
from script1 import DESTINATION_FOLDER as persistent_folder
from streamlit_extras.bottom_container import bottom

output = ""

def list_folders_in_directory(path):
    all_folders = []
    
    for root, dirs, files in os.walk(path):
        for folder in dirs:
            all_folders.append(os.path.join(root, folder))
    all_folders.sort()
    return all_folders

def print_directory_structure(path=''):
    try:
        folders = list_folders_in_directory(path)  # Ensure this function is defined
        directory_structure = ""
        for item in folders:     
            item = item.replace("/", "\\")
            directory_structure += "-" * (item.count("\\")-2) * 4 + f'{item}\n'
        return directory_structure
    except FileNotFoundError:
        return "The specified path does not exist."
    
def capture_output(func, *args, **kwargs):
    f = io.StringIO()
    with redirect_stdout(f):  # Redirect standard output to StringIO
        value = func(*args, **kwargs)
    return value, f.getvalue()

with st.sidebar:
    # Create a Streamlit app with buttons for each plot
    st.title("Run Python Scripts with Buttons")

    # Add buttons to execute each script
    if st.button("Execute idealista API get"):
        fig, output = capture_output(script0.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("Copy temporal landing files to persistent landing"):
        fig, output = capture_output(script1.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("Reset persistent zone folder"):
        fig, output = capture_output(script1.reset)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("Create formatted database from persistent landing files"):
        fig, output = capture_output(script2.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit


st.write("### Directory Structure")
directory_text = print_directory_structure(path=persistent_folder)  # Replace with the desired path
st.text_area("Directory Structure", directory_text, height=400, max_chars=None)

if bottom:
    st.subheader("Function Output")
    st.text_area("Captured Output", output, height=200)
