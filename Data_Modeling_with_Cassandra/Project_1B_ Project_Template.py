#!/usr/bin/env python
# coding: utf-8

# # Part I. ETL Pipeline for Pre-Processing the Files

# ## PLEASE RUN THE FOLLOWING CODE FOR PRE-PROCESSING THE FILES

# #### Import Python packages 

# In[5]:


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

# In[6]:


# checking your current working directory
print(os.getcwd())

# Getting current folder and subfolder event data
filepath = os.getcwd() + '/event_data'

# Create a for loop to create a list of files and collect each filepath
for root, dirs, files in os.walk(filepath):
    
# joining the file path and roots with the subdirectories using glob
    file_path_list = glob.glob(os.path.join(root,'*'))
    #print(file_path_list)


# #### Processing the files to create the data file csv that will be used for Apache Casssandra tables

# In[7]:


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
            
            
#print(len(full_data_rows_list))
#print(full_data_rows_list)

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


# In[8]:


# checking the number of rows in your csv file
with open('event_datafile_new.csv', 'r', encoding = 'utf8') as f:
    print(sum(1 for line in f))


# # Part II. Complete the Apache Cassandra coding portion of your project. 
# 
# ## Now you are ready to work with the CSV file titled <font color=red>event_datafile_new.csv</font>, located within the Workspace directory.  The event_datafile_new.csv contains the following columns: 
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
# 
# The image below is a screenshot of what the denormalized data should appear like in the <font color=red>**event_datafile_new.csv**</font> after the code above is run:<br>
# 
# <img src="images/image_event_datafile_new.jpg">

# ## Begin writing your Apache Cassandra code in the cells below

# #### Creating a Cluster

# In[9]:


# This should make a connection to a Cassandra instance your local machine 
# (127.0.0.1)

from cassandra.cluster import Cluster
cluster = Cluster()

# To establish connection and begin executing queries, need a session
session = cluster.connect()


# #### Create Keyspace

# In[10]:


session.execute("""
    CREATE KEYSPACE IF NOT EXISTS mykeyspace 
    WITH REPLICATION = 
    { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }""")


# #### Set Keyspace

# In[11]:


session.set_keyspace('mykeyspace')


# ### Now we need to create tables to run the following queries. Remember, with Apache Cassandra you model the database tables on the queries you want to run.

# ## Create queries to ask the following three questions of the data
# 
# ### 1. Give me the artist, song title and song's length in the music app history that was heard during  sessionId = 338, and itemInSession  = 4

# The sessionId column was used as a PARTITION KEY for filteration (as per requested query) the table by this column.
# 
# The itemInSession column as clustering column to sort the result.

# In[12]:


# Creating table (artist_song)
query = "CREATE TABLE IF NOT EXISTS artist_song"
query = query + "(sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY (sessionId, itemInSession))"
session.execute(query)
                    


# In[13]:


file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO artist_song (sessionId, itemInSession, artist, song, length)"
        query = query + "VALUES (%s, %s, %s, %s, %s)"
        session.execute(query, (int(line[8]), int(line[3]), line[0], line[9], float(line[5])))


# In[14]:


## SELECT to verify that the data have been inserted into each table

query = "SELECT artist, song, length from artist_song WHERE sessionId=338 AND itemInSession=4"
rows = session.execute(query)
for row in rows:
    print(row.artist, row.song, row.length)


# 
# 
# ### 2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182
#     

# The userId and sessionId column were used as a PARTITION KEY to filter on the table by these two columns.
# 
# The itemInSession column as clustering column to sort the table by this column.
# 

# In[16]:


query = "CREATE TABLE IF NOT EXISTS artist_song_user"
query = query + "(userId int, sessionId int, itemInSession int, artist text, song text, firstName text, lastName text, PRIMARY KEY ((userId, sessionId), itemInSession))"
session.execute(query)

# Insert data into table for query 2
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO artist_song_user (userId, sessionId, artist, song, itemInSession, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        session.execute(query, (int(line[10]), int(line[8]), line[0], line[9], int(line[3]), line[1], line[4]))

                    


# In[17]:


query = "SELECT artist, song, firstName, lastName FROM artist_song_user WHERE userId=10 AND sessionId=182"
rows = session.execute(query)
for row in rows:
    print(row.artist, row.song, row.firstname, row.lastname)    


# 
# 
# ### 3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

# The song column was used as a PARTITION KEY (Used for filteration as the query will be filtered on this column).
# 
# The userId column as clustering column to sort by this column.

# In[20]:


query = "CREATE TABLE IF NOT EXISTS user_song "
query = query + "(song text, userId int,firstName text, lastName text, PRIMARY KEY (song, userId))"
session.execute(query)

# Insert data into table for query 3
file = 'event_datafile_new.csv'
with open(file, encoding = 'utf8') as f:
    csvreader = csv.reader(f)
    next(csvreader)
    for line in csvreader:
        query = "INSERT INTO user_song (song, userId, firstName, lastName)"
        query = query + "VALUES (%s, %s, %s, %s)"
        session.execute(query, (line[9], int(line[10]), line[1], line[4]))


# In[21]:


query = "SELECT firstName, lastName FROM user_song WHERE song='All Hands Against His Own'"
rows = session.execute(query)
for row in rows:
    print(row.firstname, row.lastname)


# ### Dropping the tables before closing out the sessions

# In[22]:


session.execute("drop table artist_song")
session.execute("drop table artist_song_user")
session.execute("drop table user_song")


# ### Close the session and cluster connection¶

# In[23]:


session.shutdown()
cluster.shutdown()

