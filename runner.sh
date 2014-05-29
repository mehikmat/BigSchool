#!/bin/sh
#Remove output directory if already exists
$HADOOP_HOME/bin/hadoop fs -rmr output/output.txt

#Create input directory
$HADOOP_HOME/bin/hadoop fs -mkdir -p input

#Put input data file
$HADOOP_HOME/bin/hadoop fs -put ./data/input.txt input/

#Run Job
$HADOOP_HOME/bin/hadoop jar target/BigSchoolMapReduce-1.0.jar input/input.txt output/output.txt