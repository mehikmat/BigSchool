#!/bin/sh
#Remove output directory if already exists
$HADOOP_HOME/bin/hdfs dfs -rm -r Student
$HADOOP_HOME/bin/hdfs dfs -rm -r output

#Create input directory
$HADOOP_HOME/bin/hdfs dfs -mkdir -p Student

#Put input data file
$HADOOP_HOME/bin/hdfs dfs -put ./data/*.pdf Student/

#Make jar
mvn clean package

#Run Job
# App-1 "student-process"
$HADOOP_HOME/bin/hadoop jar target/BigSchoolMapReduce-1.0.jar student-process Student output