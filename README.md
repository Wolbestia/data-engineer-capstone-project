# data-engineer-capstone-project
Capstone project for Udacity Data Engineering nanodegree

## Project summary 

The goal of this project is to create an ETL and build a data lake populated with football statistics. The objective is to bring the data needed to make a deep analysis of the players performance. This project aims to answer questions like who is the most efficient team at pass rushing, which college produces the better receivers or who is the most precise quarterback in first downs. 

## Data sources

The project uses two datasets. The first one is on /nflstatistics and is a JSON file with basic information about each player. This data was scrapped from the official NFL source of stats. In the other hand there's the second dataset which was given by the NFL itself in a Big Data contest. The dataset contains player tracking data, play, game and player level information about passing plays during the 2018 season. It has only plays where the ball was thrown, there was a penalty or the quarterback was sacked. The focus of the contest was pass coverage and for that reason there's no information about linemen.

big-data-bowl dataset:
https://www.kaggle.com/c/nfl-big-data-bowl-2021/overview

### Big data bowl file information

- The games.csv contains both of the teams on each game of the season. 
- The players.csv file has information about info about every player that took part of the season.
- The plays.csv file is the main source for statistics of the datasets. It has very specific information about each of the plays during the season such as the number of pass rushers, the down, the play type or the yards gained by the offense. 
- The weeks*.csv files is the tracking data and each week of the season has its own file. In this file there are info about where the player was located in the file during each one of the plays. 

## ETL files

There's two main files for the ETL. The first one is etl.py, which imports the config and main variables and orquestates the creation of the tables. Then there's etl_functions.py with all the logic behind the ETL. The empty dl.cfg that must contain the AWS credentials. 

## Infraestructure 

The objective of this project is to create a data lake with the files stored in S3 to make it accessible and reliable. With that on mind, the tables size is considerable and that's why the files are stored in .parquet format. With a columnar format we could have TBs of data and still have a fast and reliable data lake to make the desired analysis. 

The data model is a simple star-schema with one fact table and six dimension tables. 

For the ETL process PySpark is the option due to its speed and easy-to-understand syntax. In this case we create a local cluster with SparkSession, but in a real environment a EMR cluster would be ideal. 

Finally, the data can be read with Amazon Athena to test the results, but in a real environment the data could be accessed with many BI options. 

## Step 1: Scope the project and gather data

The first idea of the project was to make an infraestructure that could bring analytics data to everyone interested on american football. With that in mind I considered bringing the data to both television or radio programs and to professional teams. The data would be so deep that it could be analyzed at many levels. 
To make that possible I would have to create a reliable data model populated with deep information. There were so many datasets on the internet, but none with enough info to make it worth. Until I found the Big Data Bowl event, when I decided that this would be the best option. 

The scope of the project was to create the full infraestructure needed to make the analysis happen and with enough investigation and reading I decided that S3, Spark and Athena would be the best option. I could have a structured star-schema and then have the full files of the dataset in S3 too. 

## Step 2: Explore and Assess the Data

This process is documented in the python notebook provided in this repository. 

## Step 3: Define the Data Model 

As I progressed in my analysis of the project, I was refining the data model. With the knowledge acquired in the exploration of the datasets I created the final data model.
![prueba](https://user-images.githubusercontent.com/25299249/120343274-dd92de80-c2f8-11eb-9bf1-2504fcfe6d5e.png)

The fact table has specific info about each player that played a snap in the season. Then there's different dimension tables for each one of the main positions of a football team. Each one of them has which I considered interesting fields or statistics for that position. For example in the quarterback dimension it's interesting to have the down and how many yards were needed to take the first down. Another example would be the filter for each of the dimension's data, where the Cornerback or Linebacker dimensions have info about intercepted, sacked or incomplete plays. 

## Step 4: Run ETL to model the data

### Prerequisites:
- Python3
- Pyspark fully configured. Here you can read how to install it: https://medium.com/tinghaochen/how-to-install-pyspark-locally-94501eefe421
- - AWS S3 bucket to store the parquet files
- nfl-big-data-bowl-2021 dataset from Kaggle
- 
To download the main dataset from Kaggle, you have to run:

```
kaggle competitions download -c nfl-big-data-bowl-2021
```

To execute the ETL you have to simply execute the main file with:
```
python3 etl.py
```
The script contains log info about the execution process and when full schema is created it runs a few data quality checks. If any of the the data quality checks fails it's displayed on the execution of the script. 

The dataset is very huge and in the first execution of the script it may take a while to create all the tables in a local Spark cluster. To test the script you could replace the write sentences: 
```
lbs_df.repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'lb_stats/lb_stats.parquet'), 'overwrite')
```
With: 
```
lbs_df.limit(100) \
        .repartition(col("team"), col("game_id")) \
        .write \
        .option("maxRecordsPerFile", 11) \
        .partitionBy("team", "game_id") \
        .parquet(os.path.join(output_data, 'lb_stats/lb_stats.parquet'), 'overwrite')
```

### Real use case

Here's a final use case with a limited part of 50 players of the whole dataset on Amazon Athena:

#### Best quarterbacks in the first downs
Here, we can see who are the best quarterbacks in the first downs based on a limited portion of the dataset. The criteria applied is the most yards gained with a complete pass.

![qb_stats_first_down_def](https://user-images.githubusercontent.com/25299249/120519700-346ce680-c3d3-11eb-81c6-31bdbb8cfe5c.png)

#### Blocking tight ends VS route-runners 
Another real case scenario would be comparing the tight ends who run routes to catch the ball or those who usually block at the line of scrimmage. Here, we can see who are the tight ends that blocks the most and the ones that are used to run routes:

![te_block_plays](https://user-images.githubusercontent.com/25299249/120638849-f4f4d780-c470-11eb-9f3f-d68b02f677f1.png)
![te_route_plays](https://user-images.githubusercontent.com/25299249/120638855-f8885e80-c470-11eb-8f24-28a99a616352.png)

#### The best QB tackler in the league
It would be very interesting who is the best player at sacking the quarterback. In these case we would like to analyze all the deffense players since the safeties and cornerbacks can sack the QB too, although it's not common. 
![deffense_sacks](https://user-images.githubusercontent.com/25299249/120639060-3eddbd80-c471-11eb-8a63-7667856b3e51.png)

## Step 5: Description of the approach of different problems

### If the data was increased by 100x
If the data was increased by 100x I would upgrade the Spark cluster by building an EMR cluster on AWS. This way the ETL would be able to handle bigger data thanks to mapreduce and an efficient cluster configuration. 

### If the pipelines were run on a daily basis by 7am
If the ETL should be run on a daily basis I would build a data pipeline on Apache Airflow. This way I would be able to schedule the execution of the ETL and debug each task separately. 

### If the database needed to be accessed by 100+ people
Athena is a good and cheap option since they charge for each query executed. Howhever if the database needed to be accessed by a lot of people I would migrate the database to Amazon Redshift. Redshift would provide a better performance when accessed by a large amount of people and it's easier to monitor the performance and debug potential problems.
