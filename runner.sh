#!/bin/sh
#Remove output directory if already exists
$HADOOP_HOME/bin/hdfs dfs -rm -r input
$HADOOP_HOME/bin/hdfs dfs -rm -r output
$HADOOP_HOME/bin/hdfs dfs -rm -r index

#Create input directory
$HADOOP_HOME/bin/hdfs dfs -mkdir -p input

#Put input data file
$HADOOP_HOME/bin/hdfs dfs -put ./data/Student.csv input/

#Make jar
mvn clean package

#Run Job
# App-1 "student-process"
$HADOOP_HOME/bin/hadoop jar target/BigSchoolMapReduce-1.0.jar student-process input output
#$HADOOP_HOME/bin/hadoop jar target/BigSchoolMapReduce-1.0.jar output index