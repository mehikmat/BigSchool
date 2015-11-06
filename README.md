BigSchool [![endorse](https://api.coderwall.com/mehikmat/endorsecount.png)](https://coderwall.com/mehikmat) [![Build Status](https://travis-ci.org/mehikmat/BigSchool.svg)](https://travis-ci.org/mehikmat/BigSchool)
=========

MapReduce-2 Application for testing custom pdf input format

Technologies: Cloudera Hadoop CDH5,Maven, IntelliJ IDEA

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

    $ git checkout custom_input_format<branch_name>
    
    $ mvn clean package

    $ yarn jar target/BigSchoolMapReduce-1.0.jar Student output

      OR

    $ sh runner.sh
