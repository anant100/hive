# hive
Design a full batch data pipeline  ||  Use Hive to prepare raw data for data transformation  ||  Partitioning (sharding) in Hive

# Data set
I use STM GTFS data set. In this project, we continue the practice of enrichment (the most common data transformation task to do). To download the dataset, visit http://www.stm.info/en/about/developers

# Problem statement
We get the information of STM every day and need to run an ETL pipeline to enrich data for reporting and analysis purpose in once a day batch job.

# Description

## Data pipeline installation

-> Path on HDFS: /user/cloudera/[GROUP] /project4/[YOUR NAME] where [GROUP] is the summer2019.

-> Create a directory for each source table called /user/cloudera/[GROUP] /project4/[YOUR NAME]/[TABLE NAME] where [TABLE NAME] is from the following list

•	trips

•	calendar_dates

•	frequencies

-> Create a database called [YOUR NAME] in Hive. If you already have one, just use and don’t try to create multiple databases.

-> Create staging tables called ext_[TABLE NAME]. Staging tables are external tables that point to staging directory of each source.

•	trips

•	calendar_dates

•	frequencies
Note that the LOCATION for each table is HDFS path under staging directory.

-> Create a managed table called enriched_trip with Parquet encoding and partitioned by wheelchair_accessible


## Extract Data from STM to Staging area

-> Download the data set of STM GTFS from http://stm.info/sites/default/files/gtfs/gtfs_stm.zip

-> Put extracted version into /user/cloudera/[GROUP] /project4/[YOUR NAME]/[TABLE NAME] path on HDFS where [TABLE NAME] here is the name of file without extension.
We just need the following tables

•	trips
•	calendar_dates
•	frequencies


## Data Pipeline

Enrich trips with calendar dates and frequencies and write it to the enriched_trip table.
You could take any of the following options:
1.	Run a SQL query with dynamic partition using beeline command line
2.	Implement a Scala/Java application and run the SQL query using Hive JDBC
3.	Use the project of course 2 and 3 and customize it to write the files under proper partition folder on HDFS. Then use MSCK REPAIR command to recover partitions. The latter can be done using Hive JDBC or using beeline.


# Bonus

-	Optimized JOIN to get enriched trips information with explanation in demo session
-	Automating schema deployment (using shell script or any other scripting language)
-	Automating staging phase (using shell script or any other scripting language)
