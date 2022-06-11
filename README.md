# Proj-2-Pop-vs-Oly
Project 2: Population vs Olympic Medals

This repo contains information on an ETL project (Extract-Transform-Load). The research included extracting two datasets from Kaggle, once containing annual population data by country, with the other containing medal counts for each Summery Olympic games of the modern era (since 1896). The datasets where cleaning, merged, and loaded into a PostgreSQL table. 

Future analysis may include studying the relationship between a country's population and its medal-winning success (or lack thereof).  

An overview of the ETL process (as described herein) is as follows:

![Project Flowchart](https://github.com/VTNoble/Proj-2-Pop-vs-Oly/blob/main/Resources/Flowchart.jpg?raw=true)


Files included in this repo include:
* Resources folder containing two csv files of the raw Kaggle datasets
* data_etl.ipynb used for initial development of data transformation step
* data_etl.py script for deployment of the Jupyter notebook described above
* queries.sql file for creating the tables utilized in the final step of this process


