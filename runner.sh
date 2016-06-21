#!/bin/sh
#Remove output directory if already exists
hadoop fs -rm -r input
hadoop fs -rm -r output

#Create input directory
hadoop fs -mkdir -p input

#Put input data file
hadoop fs -put ./data/input.txt input/

#build jar
mvn clean package

#Run Job
hadoop jar target/BigSchoolCascading-1.0.jar input/input.txt output/output.txt
