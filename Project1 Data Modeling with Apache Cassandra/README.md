# Project: Data Modeling with Apache Cassandra

## Background
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
The analysis team is particularly interested in understanding what songs users are listening to. 
Currently, there is no easy way to query the data to generate the results, since the data reside in a directory of CSV files on user activity on the app.

Your role is to create an Apache Cassandra database which can create queries on song play data to answer the questions and do analysis. 
You'll be able to test your database by running queries given to you by the analytics team from Sparkify to create the results.

## Project Overview
You'll apply data modelling with Apache Cassandra and complete an ETL pipeline using python.
To complete the project, you will need to model your data by creating tables in Apache Cassandra to run queries. 
You will need to create an ETL pipeline that transfers data from a set of CSV files within a directory to create a streamlined CSV file 
to model and insert data into Apache Cassandra tables.

## Dataset
you'll be working with one dataset: event_data. The directory of CSV files partitioned by date. 

## Steps
- you will process the event_datafile_new.csv dataset to create a denormalized dataset.
- you will model a no SQL database/ Apache Cassandra database data tables keeping in mind the queries you need to run.
- you will load the data into tables you create in Apache Cassandra and run your queries.
