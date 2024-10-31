# MDS-ADSDB-Proj1-JiJonas

## Instructions

**Repository and Cloning Instructions**

The project is hosted on GitHub at the following repository: [MDS-ADSDB-Proj1-JiJonas](https://github.com/Quiracle/MDS-ADSDB-Proj1-JiJonas). To clone the repository and set up the project on your local machine, follow these steps:

1. **Clone the Repository**  
   Open your terminal and run the following command:
   ```bash
   git clone https://github.com/Quiracle/MDS-ADSDB-Proj1-JiJonas.git
   ```
   This will create a local copy of the repository in a folder named `MDS-ADSDB-Proj1-JiJonas`.

2. **Navigate to the Project Directory**  
   Move into the cloned repository folder:
   ```bash
   cd MDS-ADSDB-Proj1-JiJonas
   ```

3. **Docker Setup**  
   As described, we provide two Docker Compose configurations:
   - **With Volume**: To keep generated files synced with your local machine:
     ```bash
     docker-compose -f docker-compose.yml up
     ```
   - **Without Volume**: To test in an isolated environment:
     ```bash
     docker-compose -f docker-compose-non-persistent.yml up
     ```

4. **Access the Streamlit UI**  
   Once the Docker container is running, open your browser and go to `http://localhost:8501` to access the Streamlit interface, where you can execute tasks and monitor the project.

This setup ensures that you have all the necessary tools and configurations to run the project seamlessly on your local environment. 