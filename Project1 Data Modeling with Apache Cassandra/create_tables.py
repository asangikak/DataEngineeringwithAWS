#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[14]:


# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv


# #### Creating list of filepaths to process original event csv data files

# In[15]:


# checking the current working directory
print(os.getcwd())

# Get the current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# join the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[16]:


# initiating an empty list of rows that will be generated from each file
full_data_rows_list = [] 
    
# for every filepath in the file path list 
for f in file_path_list:

# reading csv file 
    with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
        # creating a csv reader object 
        csvreader = csv.reader(csvfile) 
        next(csvreader)
        
 # extracting each data row one by one and append it        
        for line in csvreader:
            #print(line)
            full_data_rows_list.append(line) 
            
# creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
# Apache Cassandra tables
csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
    writer = csv.writer(f, dialect='myDialect')
    writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',                'level','location','sessionId','song','userId'])
    for row in full_data_rows_list:
        if (row[0] == ''):
            continue
        writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


# In[17]:


# check the number of rows in the csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. The event_datafile_new.csv contains the following columns: 
# - artist 
# - firstName of user
# - gender of user
# - item number in session
# - last name of user
# - length of the song
# - level (paid or free song)
# - location of the user
# - sessionId
# - song title
# - userId

# #### Creating a Cluster

# In[46]:


# Make a connection to a Cassandra instance the local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[47]:


# TO-DO: Create a Keyspace 
try:
    session.execute("""
    CREATE KEYSPACE IF NOT EXISTS udacity
    WITH REPLICATION =
    { 'class': 'SimpleStrategy', 'replication_factor' : 1}"""
    )
except Exception as e:
    print(e)


# #### Set Keyspace

# In[48]:


# TO-DO: Set KEYSPACE to the keyspace specified above
try:
    session.set_keyspace('udacity')
except Exception as e:
    print(e)


# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4
# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# 
# 
# 

# In[23]:


# Read the csv file to a pandas dataframe and see what the resulting dataframe should look like
df0 =  pd.read_csv("event_datafile_new.csv")
df0.loc[(df0['sessionId'] == 338) & (df0['itemInSession'] == 4)]


# In[24]:


## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
## sessionId = 338, and itemInSession = 4

query = "CREATE TABLE IF NOT EXISTS music_app_session_history"
query = query + "(session_id text, item_in_session text, artist text, song text, song_length text,                     PRIMARY KEY (session_id, item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)


# In[25]:


file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
##  Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_app_session_history (session_id, item_in_session, artist, song, song_length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (line[8], line[3], line[0], line[9], line[5]))


# #### Do a SELECT to verify that the data have been inserted into each table

# In[26]:


## Add in the SELECT statement to verify the data was entered into the table

query = "SELECT artist, song, song_length FROM music_app_session_history WHERE session_id = '338' AND item_in_session ='4' "
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist, row.song, row.song_length)


# ### COPY AND REPEAT THE ABOVE THREE CELLS FOR EACH OF THE THREE QUESTIONS

# In[27]:


# Read the csv file to a pandas dataframe and see what the resulting dataframe should look like
df =  pd.read_csv("event_datafile_new.csv")
df.loc[(df['userId'] == 10) & (df['sessionId'] == 182)]


# In[36]:


## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
## for userid = 10, sessionid = 182


query = "CREATE TABLE IF NOT EXISTS music_app_artist_history"
query = query + "(artist text, first_name text, item_in_session text, last_name text, session_id text,                     song text, user_id text,                     PRIMARY KEY (user_id, session_id, item_in_session))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# set up the CSV file
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_app_artist_history (user_id, session_id, item_in_session,                     artist, first_name, last_name, song)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (line[10], line[8], line[3], line[0], line[1], line[4], line[9]))

        
# Add in the SELECT statement to verify the data was entered into the table

query = "SELECT artist, song, first_name, last_name FROM music_app_artist_history             WHERE user_id = '10' AND session_id = '182'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.artist, row.song, row.first_name, row.last_name)                   


# In[37]:


# Read the csv file to a pandas dataframe and see what the resulting dataframe should look like
df1 =  pd.read_csv("event_datafile_new.csv")
df1.loc[(df1['song'] == 'All Hands Against His Own')]


# In[43]:


## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
# song, user_id combination is unique.Therefore it is chosen as the primary key to the table
query = "CREATE TABLE IF NOT EXISTS music_app_song_history"
query = query + "(first_name text, last_name text, session_id text, song text, user_id text,                     PRIMARY KEY (song, user_id))"
try:
    session.execute(query)
except Exception as e:
    print(e)

# set up the CSV file
file = 'event_datafile_new.csv'

with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader) # skip header
    for line in csvreader:
## Assign the INSERT statements into the `query` variable
        query = "INSERT INTO music_app_song_history (song, user_id, first_name, last_name, session_id)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        ## Assign which column element should be assigned for each column in the INSERT statement.
        session.execute(query, (line[9], line[10], line[1], line[4], line[8]))
        
# Add in the SELECT statement to verify the data was entered into the table

query = "SELECT first_name, last_name FROM music_app_song_history WHERE song='All Hands Against His Own'"
try:
    rows = session.execute(query)
except Exception as e:
    print(e)
    
for row in rows:
    print(row.first_name, row.last_name)                   
                    


# In[ ]:





# ### Drop the tables before closing out the sessions

# In[13]:


## Drop the table before closing out the sessions


# In[49]:


query = "DROP TABLE IF EXISTS music_app_artist_history"

try:
    session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS music_app_artist_history"

try:
    session.execute(query)
except Exception as e:
    print(e)

query = "DROP TABLE IF EXISTS music_app_song_history"

try:
    session.execute(query)
except Exception as e:
    print(e)


# ### Close the session and cluster connection¶

# In[50]:


session.shutdown()
cluster.shutdown()

