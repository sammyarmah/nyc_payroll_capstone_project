# Project Introduction
The City of New York would like to develop a Data Analytics platform on Azure Synapse Analytics to accomplish two primary objectives:

Analyze how the City's financial resources are allocated and how much of the City's budget is being devoted to overtime.
Make the data available to the interested public to show how the City’s budget is being spent on salary and overtime pay for all municipal employees.
You have been hired as a Data Engineer to create high-quality data pipelines that are dynamic, can be automated, and monitored for efficient operation. The project team also includes the city’s quality assurance experts who will test the pipelines to find any errors and improve overall data quality.

Database Design
The data is modeled using a star schema, consisting of:

Fact Table:

payroll_facts_table: Contains payroll details such as employee ID, payroll number, salary information, and more.
Dimension Tables:

employee_dim: Stores employee details (EmployeeID, FirstName, LastName).
title_dim: Stores job title information (TitleCode, TitleDescription).
agency_dim: Stores agency information (AgencyID, AgencyName).

ER Diagram

![data_model drawio](https://github.com/user-attachments/assets/963cd6f7-984a-4279-a731-7a6fce76661d)

ETL Pipeline
The ETL process involves extracting data from CSV files, transforming it to ensure consistency and accuracy, and loading it into a PostgreSQL database.

Key Steps:
Extract: Read data from CSV files.
Transform: Perform data cleaning, format adjustments, and dimensional modeling (fact and dimension tables).
Load: Insert the transformed data into the PostgreSQL database.


