BigSchool
=========

MapReduce-1 Application for processing raw data file and counting words

Technologies: Cloudera Hadoop CDH3,Maven, IntelliJ IDEA

###NOTE:MRv1 & MRv2 compatibility###
```
In general, the previous MapReduce runtime (aka MRv1) has been reused and no major surgery has been conducted on it.
Therefore, MRv2 is able to ensure satisfactory compatibility with MRv1 applications.
However, due to some improvements and code refactorings, a few APIs have been rendered backward-incompatible.

NEW API in package: org.apache.hadoop.mapreduce;
OLD API in package: org.apache.hadoop.mapred;
```

Prerequisites
===============
1. Java-1.6
2. Maven-2/3
3. git
4. Hadoop-0.20.*

How to run
===============
    $ git clone https://github.com/mehikmat/BigSchool.git

    $ cd BigSchool

    $ mvn clean package

    $ hadoop jar target/BigSchoolMapReduce-1.0.jar input/input.txt output/output.txt

      OR

    $ sh runner.sh

  In case you get an error of type "Not valid JAR", check the jar path. It might be different from one plateform to another with mvn.


Browse http://localhost:50030 for job status


