# Proj-2-Pop-vs-Oly

This repo contains information on an ETL project (Extract-Transform-Load). The research process included two datasets from Kaggle, one containing annual population data by country, with the other featuring medal counts for each Summer Olympic games of the modern era (since 1896). The datasets where cleaning, merged, and loaded into a PostgreSQL table. 

While the scope of this research was limited to an ETL process, future analysis of this dataset may include studying the relationship between a country's population and its Olympic medal-winning success (or lack thereof).  

An overview of the ETL process (as described herein) is as follows:

![Project Flowchart](https://github.com/VTNoble/Proj-2-Pop-vs-Oly/blob/main/Resources/Flowchart.jpg?raw=true)


Files included in this repo include:
* Technical Report-Project 2, which includes greater detail of this ETL process
* Resources folder containing two csv files of the raw Kaggle datasets
* data_etl.ipynb used for initial development of data transformation step
* data_etl.py script for deployment of the Jupyter notebook described above
* queries.sql file for creating the tables utilized in the final step of this process


