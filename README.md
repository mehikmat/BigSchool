BigSchool [![endorse](https://api.coderwall.com/mehikmat/endorsecount.png)](https://coderwall.com/mehikmat)
=========
MapReduce Application for processing raw data file and counting words
Technologies: Cloudera Hadoop CDH3, Cascading, Maven, IntelliJ IDEA
Technologies: Cloudera Hadoop CDH3, Cascading, Maven, ElasticSerach, IntelliJ IDEA

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

    $ git checkout yarn-app

    $ mvn clean package

    $ $HADOOP_HOME/bin/hdfs dfs -rm  /BigSchoolYarn-1.0.jar
    $ $HADOOP_HOME/bin/hdfs dfs -put target/BigSchoolYarn-1.0.jar /
    $ $HADOOP_HOME/bin/yarn jar target/BigSchoolYarn-1.0.jar
        OR
    $ sh runner.sh

  In case you get an error of type "Not valid JAR", check the jar path. It might be different from one plateform to another with mvn.


Browse http://localhost:8088 for job status

REFERENCES:
-----------
- https://github.com/DemandCube/yarn-app
- http://tzulitai.wordpress.com/2013/09/04/applicationmaster-yarn-applications-code-level-breakdown
- http://tzulitai.wordpress.com/2013/08/30/yarn-applications-code-level-breakdown-client
- hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
