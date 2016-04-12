#!/bin/sh
#Remove output directory if already exists
$HADOOP_HOME/bin/hdfs dfs -rm -r input
$HADOOP_HOME/bin/hdfs dfs -rm -r output

#Create input directory
$HADOOP_HOME/bin/hdfs dfs -mkdir -p input

#Put input data file
$HADOOP_HOME/bin/hdfs dfs -put ./data/File1.csv input/
$HADOOP_HOME/bin/hdfs dfs -put ./data/File2.csv input/

#Make jar
mvn clean package

#Run Job
# App-1 "student-process"
$HADOOP_HOME/bin/hadoop jar target/BigSchoolMapReduce-1.0.jar input/File1.csv input/File2.csv output