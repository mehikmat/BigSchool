#!/bin/sh
#Run Job
mvn clean package
$HADOOP_HOME/bin/hdfs dfs -rm /BigSchoolYarn-1.0.jar
$HADOOP_HOME/bin/hdfs dfs -put target/BigSchoolYarn-1.0.jar /
$HADOOP_HOME/bin/yarn jar target/BigSchoolYarn-1.0.jar
