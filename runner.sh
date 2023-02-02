#!/bin/sh
#Run Job
echo "mvn clean ............"
mvn clean package -DskipTests
echo "MK dirs ............"
mkdir -p input/Eligibility
mkdir -p input/Medical
mkdir -p input/Pharmacy
echo "run generate data ............"
rm -rf input
java -jar target/BigSchoolCascading-1.0.jar generate
echo "Copy data to hdfs ............"
hadoop fs -rmr input
hadoop fs -rmr output
hadoop fs -put input .
echo "Running main job ............"
hadoop jar target/BigSchoolCascading-1.0.jar merged
