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

    $ git checkout cascading-app

    $ mvn clean package

    $ hadoop jar target/BigSchoolCascading-1.0.jar input/input.txt output/output.txt
        OR
    $ sh runner.sh

  In case you get an error of type "Not valid JAR", check the jar path. It might be different from one plateform to another with mvn.


Browse http://localhost:8088 for job status


Three dependencies must be added to the project settings.
---------------------------------------------------------
 *  First, the fluid-api and fluid-api-runtime which contains the root Fluid class.
 *  Second, the dependency that corresponds to the version of Cascading you wish to use.
 *  Third, Fluid depends on Flapi. If you are using Maven/Ivy/Gradle or similar then all
    you need to do is add the Maven repository at UnquietCode.com to your build definition
    and the necessary Flapi jars will be included in your project.


REFERENCES:
-----------
- http://www.cascading.org/support/compatibility/
- http://www.cascading.org

