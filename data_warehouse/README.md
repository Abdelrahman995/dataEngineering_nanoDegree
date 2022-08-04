# Project 3: Data warehousing with Amazon S3 and Amazon Redshift
-------------------------

### Introduction

project scope:  To help one music streaming startup, Sparkify, to move their user base and song database processes on to the cloud. Specifically, I build an ETL pipeline that extracts their data from **AWS S3** (data storage), stages tables on **AWS Redshift** (data warehouse with *columnar storage*), and execute **SQL** statements that create the analytics tables from these staging tables.

### Datasets
Datasets used in this project are provided in two public **S3 buckets**. One bucket contains info about songs and artists, the second bucket has info concerning actions done by users (which song are listening, etc.. ). The objects contained in both buckets 
are *JSON* files. 

The **Redshift** service is where data will be ingested and transformed, in fact though `COPY` command we will access to the JSON files inside the buckets and copy their content on our *staging tables*.


### Database Schema
We have two staging tables which *copy* the JSON file inside the  **S3 buckets**.
#### Staging Table 
+ **staging_songs** - info about songs and artists
+ **staging_events** - actions done by users (which song are listening, etc.. )

I've created data model of star schema optimized for queries on song play analysis. This includes the following tables.

#### Fact Table 
+ **songplays** - records in event data associated with song plays i.e. records with page `NextSong`

#### Dimension Tables
+ **users** - users in the app
+ **songs** - songs in music database
+ **artists** - artists in music database
+ **time** - timestamps of records in **songplays** broken down into specific units


### Data Warehouse Configurations and Setup step by step
* Make a new user (DWH user) `IAM user` in your AWS account and give it admin policy.
* Give it AdministratorAccess and Attach policies
* Use access key and secret key to create clients for `EC2`, `S3`, `IAM`, and `Redshift`.
* Create a role ex. `myRedShiftRole`  --> `IAM Role` that makes `Redshift` able to access `S3 bucket` (AmazonS3ReadOnlyAccess) Copy ARN for later use.
* Create a `RedShift Cluster` and get the `DWH_ENDPOIN(Host address)` and `DWH_ROLE_ARN` and fill the config file (dwh.cfg).
* Will need Host ex. redshift-test-cluster.xxxxxxxxx.region.redshift.amazonaws.com  and will find it in End Point URL
* DB_NAME will be the initial database name 
* DB_USER master admin user and its password in DB_PASSWORD
* DB_PORT is default 5439


### ETL Pipeline
+ Created tables on redshift to store the data from `S3 bucket`.
+ Loading the data from `S3 buckets` to staging tables in the `Redshift Cluster`.
+ Inserted data into fact and dimension tables from the staging tables.

### Project Structure

+ `create_tables.py` - This script will drop old tables (if exist) ad re-create new tables.
+ `etl.py` - This script executes the queries that extract `JSON` data from the `S3 bucket` and ingest them to `Redshift`.
+ `sql_queries.py` - The file contains variables with SQL statements in String formats, partitioned by `CREATE`, `DROP`, `COPY` and `INSERT` statement.
+ `dhw.cfg` - Configuration file of `Redshift`, `IAM` and `S3`

### How to Run

1- Drop and recreate tables

```bash
$ python create_tables.py
```

2- Run ETL pipeline

```bash
$ python etl.py
```