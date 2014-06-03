BigSchool [![endorse](https://api.coderwall.com/mehikmat/endorsecount.png)](https://coderwall.com/mehikmat)
=========
Cascading Application for processing raw data file and counting words
Technologies: Cloudera Hadoop CDH5, Cascading, Maven, IntelliJ IDEA

Prerequisites
===============
1. Java-1.7
2. Maven-2/3
3. git
4. Hadoop-2.3.0

How to run
===============
    $ git clone https://github.com/mehikmat/BigSchool.git

    $ cd BigSchool

    $ git checkout cascading2.5.3-app

    $ mvn clean package

    $ hadoop jar target/BigSchoolCascading-1.0.jar input/input.txt output/output.txt
        OR
    $ sh runner.sh

  In case you get an error of type "Not valid JAR", check the jar path. It might be different from one plateform to another with mvn.


Browse http://localhost:8088 for job status

REFERENCES:
-----------
- http://www.cascading.org/support/compatibility/
- http://www.cascading.org

