import streamlit as st
import io
from contextlib import redirect_stdout
import os
from scripts import script0, script1, script2, script2_1, script3, script3_1, script3_2, script3_3, script3_4, script4, script4_1, script4_2
from scripts.script1 import DESTINATION_FOLDER as persistent_folder
from streamlit_extras.bottom_container import bottom
from PIL import Image

output = ""

# Function to list images in a directory
def list_images(directory):
    images = []
    if os.path.exists(directory):
        for filename in os.listdir(directory):
            if filename.endswith(('.png', '.jpg', '.jpeg', '.gif')):
                images.append(os.path.join(directory, filename))
    return images

def list_image_folders(main_directory):
    if os.path.exists(main_directory):
        return [f.name for f in os.scandir(main_directory) if f.is_dir()]
    return []

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
view_type = st.sidebar.radio("Select View", ["Text View", "Image View"])
with st.sidebar:
    # Create a Streamlit app with buttons for each plot
    st.title("Run Python Scripts with Buttons")

    # Add buttons to execute each script
    if st.button("0 - Execute idealista API get"):
        fig, output = capture_output(script0.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("1 - Load persistent landing zone"):
        fig, output = capture_output(script1.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("1.1 - Reset persistent zone"):
        fig, output = capture_output(script1.reset)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("2 - Load fomatted zone"):
        fig, output = capture_output(script2.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("2.1 - Apply profiling on formatted zone"):
        fig, output = capture_output(script2_1.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("3 - Load Trusted zone"):
        fig, output = capture_output(script3.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("3.1 - Apply profiling on trusted zone"):
        fig, output = capture_output(script3_1.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("3.2 - Apply deduplication on trusted zone"):
        fig, output = capture_output(script3_2.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("3.3 - Apply consistent formatting on trusted zone"):
        fig, output = capture_output(script3_3.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("3.4 - Remove outliers from trusted zone"):
        fig, output = capture_output(script3_4.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("4 - Load exploitation zone"):
        fig, output = capture_output(script4.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit

    if st.button("4.1 - Apply profiling on exploitation zone"):
        fig, output = capture_output(script4_1.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit   

    if st.button("Get KPIs"):
        fig, output = capture_output(script4_2.run)  # Call the plot function from plot1.py
        st.write(fig)  # Display the figure in Streamlit     


if view_type == "Text View":
    # Show directory structure as text
    st.write(f"### Persistent landing file structure")
    directory_text = print_directory_structure(path=persistent_folder)
    st.text_area("Persistent landing file structure", directory_text, height=300, max_chars=None)

elif view_type == "Image View":
    # Dropdown selector for folders
    available_folders = list_image_folders("./plots")
    selected_folder = st.selectbox("Select a folder:", available_folders, key="folder_selector")

    # Show list of images from the directory
    st.write(f"### Profiling - Images")
    images = list_images(f"./plots/{selected_folder}")
    if images:
        # Initialize session state for the image index
        if "image_index" not in st.session_state:
            st.session_state.image_index = 0

        # Layout with three columns for navigation buttons and image
        col1, col2, col3 = st.columns([1, 6, 1])

        with col1:
            if st.button("⬅️", key="previous") and st.session_state.image_index > 0:
                st.session_state.image_index -= 1

        with col2:
            # Display the selected image in the center column
            img = Image.open(images[st.session_state.image_index])
            st.image(img, caption=os.path.basename(images[st.session_state.image_index]), use_column_width=True)

        with col3:
            if st.button("➡️", key="next") and st.session_state.image_index < len(images) - 1:
                st.session_state.image_index += 1
    else:
        st.write("No images found in the specified directory.")

if bottom:
    st.subheader("Function Output")
    st.text_area("Captured Output", output, height=200)
