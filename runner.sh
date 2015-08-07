#!/bin/sh
#Remove output directory if already exists
$HADOOP_HOME/bin/hadoop fs -rm -r input
$HADOOP_HOME/bin/hadoop fs -rm -r output

#Create input directory
$HADOOP_HOME/bin/hadoop fs -mkdir -p input

#Put input data file
$HADOOP_HOME/bin/hadoop fs -put ./data/input.txt input/

#build jar
mvn clean package

#Run Job
For uploading data to redis, for later use
$HADOOP_HOME/bin/hadoop jar target/BigSchoolCascading-1.0.jar upload_to_redis input/datatobecached.txt
Use redis cache to filter data from input file
$ hadoop jar target/BigSchoolCascading-1.0.jar use_redis_cache input/input.txt output/output.txt