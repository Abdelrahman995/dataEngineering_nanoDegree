# Project 4:  Data Lake Project 
-------------------------

This project is aiming to build an ETL pipeline that will be able to extract data from an S3 bucket,then processing the data using Spark and load the data back into s3 as a set of dimensional tables in spark parquet files.

## Database Schema Design
By building star schema contains one fact table, `songplays` and four dimensional tables namely `users`, `songs`, `artists` and `time`. which will contain clean data that is suitable for OLAP(Online Analytical Processing) operations.


### Introduction

project scope:   .

### Datasets

    1- Song Dataset in JSON format
    Below is an example of what a single song file, **TRAABJL12903CDCF1A.json**
    ```
    {
        "num_songs": 1, 
        "artist_id": "ARJIE2Y1187B994AB7", 
        "artist_latitude": null, 
        "artist_longitude": null, 
        "artist_location": "", 
        "artist_name": "Line Renaud", 
        "song_id": "SOUPIRU12A6D4FA1E1", 
        "title": "Der Kleine Dompfaff", 
        "duration": 152.92036, 
        "year": 0    
    }
    ```
    2- Log Dataset in JSON format

### To Create aws EMR cluster command line

install AWS CLI from This URL https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html

```bash
aws emr create-cluster --name spark_cluster_test \
 --use-default-roles --release-label emr-5.28.0  \
--instance-count 3 --applications Name=Spark Name=Zeppelin  \
--bootstrap-actions Path="s3://bootstrap.sh" \
--ec2-attributes KeyName=<Key-pair-file-name>, SubnetId=<subnet-Id> \
--instance-type m5.xlarge --log-uri s3:///emrlogs/
```


## <br>Project Files


In addition to the data files, the project workspace includes 5 files:

**1. dl.cfg**                      Contains the Secret Key for AWS access [Configurations] <br>
**2. create_bucket.py**            Create bucket in AWS S3 to store the extracted dimentional tables.<br>
**3. etl.py**                      Loading song data and log data from S3 to Spark, transforms data into a set of dimensional tables, then save the table back to S3 <br>
**4. etl.ipynb**                   Used to design ETL pipelines <br>
**5. README.md**                   Provides project info<br>



## Build ETL Pipeline

**etl.py** will process the entire datasets.

### Before Run 
Set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.

## Instruction

1. Set **key** and **secrect** in **dwh.cfg** file <br><br>

2. Use following command to start ETL process <br>
    **python etl.py** <br> <br>