#!/bin/sh
#Remove output directory if already exists
$HADOOP_HOME/bin/hdfs dfs -rm -r output/output.txt

#Create input directory
$HADOOP_HOME/bin/hdfs dfs -mkdir -p input

#Put input data file
$HADOOP_HOME/bin/hdfs dfs -put ./data/input.txt input/

#Make jar
mvn clean package

#Run Job
$HADOOP_HOME/bin/hadoop jar target/BigSchoolMapReduce-1.0.jar input/input.txt output/output.txt