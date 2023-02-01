#!/bin/sh
#Run Job
hadoop jar target/BigSchoolCascading-1.0.jar -D mapreduce.map.log.level=ERROR -D mapreduce.reduce.log.level=ERROR
