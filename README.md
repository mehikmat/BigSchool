BigSchool
=========

BigData Application for processing universities raw data and write processed data to elasticsearch

Technologies: Cloudera Hadoop CDH3, Cascading, Maven, ElasticSerach, IntelliJ IDEA

Prerequisites
===============
1. Java-1.6
2. Maven-2/3
3. git
4. Hadoop

How to run
===============
    $ git clone https://github.com/mehikmat/BigSchool.git

    $ cd BigSchool

    $ mvn clean package

    $ hadoop jar target/BigSchool-1.0.jar com.bigschool.driver.App input/input.txt output/output.txt
                     OR
    $ sh runner.sh
                     OR
    $ hadoop jar target/BigSchoolMapReduce-1.0.jar target/classes/com/bigschool/driver/App.class input/input.txt output/output.txt

  In case you get an error of type "Not valid JAR", check the jar path. It might be different from one plateform to another with mvn.


Browse http://localhost:50030 for job status


