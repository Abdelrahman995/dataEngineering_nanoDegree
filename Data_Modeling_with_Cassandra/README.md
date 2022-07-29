# **Data Modeling with Apache Cassandra**
-----------------------------------------------------------------------------------------------------------------------------------

In this project, Using Python to build simple ETL pipeline from csv to be ingested in Apache cassandra tables.

Cassandra primary keys is made up of Partition Keys or Clustering columns.
ex. In creation statement

#### Creating table Example
CREATE TABLE IF NOT EXISTS artist_song
(sessionId int, itemInSession int, artist text, song text, length float, PRIMARY KEY (sessionId, itemInSession))
both (sessionId, itemInSession) are primary keys they should be unique

In Cassandra the denormalization process is a must, you cannot apply normalization like an RDBMS.
Apache Cassandra has been optimized for fast writes not for fast reads.

By processing csv file "event_datafile_new.csv" dataset to be ingested in Cassandra table.


# **Motivation**
-----------------------------------------------------------------------------------------------------------------------------------

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team at Sparkify whats to know what songs users are listening to. In order to analyze songs and user activity they need to query certain data, however, the data they currently have is is JSON format.



# **Files**
-----------------------------------------------------------------------------------------------------------------------------------

## Project Structure

* event_data - The directory of CSV files partitioned by date

* Project_1B_ Project_Template.ipynb - It is a notebook that illustrates the project step by step

* event_datafile_new.csv - The aggregated CSV composed by all event_data files


# **Requirements**
-----------------------------------------------------------------------------------------------------------------------------------
Cassandra instance needs to be up and running
Python
Jupyter Notebook (for development).
