# Distributed Database Management Systems Final Project
Welcome to the repository for the final project in the Distributed Database Management Systems course. In this project, I will be working on two distinct parts, each with its own set of tasks and challenges. Below, you will find an overview of the project, its objectives, and the structure of this repository.

## Project Overview
### Part 1: Cinema Database Redesign
#### Background
For this part of the project, I am assisting the "Distributed Theaters" cinema company, the second-largest cinema company in Israel, with 70 cinemas across the country. Their goal is to re-design their database to maximize efficiency and increase sales. The data is sourced from The Movies Dataset on Kaggle.

#### Tasks
##### Extract and Transform:
Load data, preprocess it, and apply transformations using Spark. This step is preliminary to the analysis and implementation parts. The Python script and a PDF describing your data store and transformations should be submitted.

##### Data Analysis:
Analyze the dataset and derive at least 4 insights using Spark. Insights should consider user behavior in ticket buying and query patterns. Visualizations are encouraged. Submit a Jupyter notebook with your analysis and an HTML version of the notebook.

##### Design:
Design a distributed database based on your analysis insights. You may use algorithms from the lectures and modify them as needed. Submit a PDF describing your design considerations and decisions.

##### Implementation:
Implement your design using PySpark, dividing it into five directories corresponding to different cities. Each directory should contain fragments of each site according to your design. Submit a Python file with the implementation.

### Part 2: Human Health Lifestyle Project
#### Background
In this part, I assist the "Distributed Human Health Association," which aims to improve people's lifestyles. They have collected data on daily activities of users through smartphones and smartwatches.

#### Tasks
##### Data Analysis:
Investigate the data and derive 3 insights based on different aspects of the data. Use visualizations where applicable. Submit a Jupyter notebook with your analysis and an HTML version of the notebook.

##### Learning Task:
Build a Machine Learning model using Spark MLlib to predict the activity variable ('gt'). Define training and testing sets, create and train the model, and evaluate it using accuracy. Submit a PDF description of your learning task and a Python file with the model implementation.

##### Extract and Learn:
Use Spark Streaming to extract data from a data source while simultaneously training and testing the ML model. Constraint: The model cannot predict data it was trained on, and it must predict all incoming data. Show the model's performance throughout the streaming process.

## Repository Structure
This repository is organized into folders and files corresponding to each part of the project:

### Part1: Contains files and folders related to the Cinema Database Redesign.
01_Data Extractions_And_Transformations

02_Data_Analysis

03_Distributed_Database_Design

04_Distributed_Database_Implementation
### Part2: Contains files and folders related to the Human Health Lifestyle Project.
01_Data_Analysis_And_Learning_Task

02_Straming_And_Learning

Please refer to each respective folder for detailed information, code, and documentation related to each part of the project.
For any questions or clarifications, please feel free to contact me :)

Thank you for exploring my project repository!
