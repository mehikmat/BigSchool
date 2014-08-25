BigSchool [![endorse](https://api.coderwall.com/mehikmat/endorsecount.png)](https://coderwall.com/mehikmat)
=========
Simple YARN application to run n copies of a unix command

Technologies: Cloudera Hadoop CDH5, Maven, IntelliJ IDEA

Technologies: Cloudera Hadoop CDH5, Maven, IntelliJ IDEA

Prerequisites
===============
1. Java-1.6
2. Maven-2/3
3. git
4. Hadoop-2.3.*

How to run
===============
    $ git clone https://github.com/mehikmat/BigSchool.git

    $ cd BigSchool

    $ git checkout yarn-app

    $ mvn clean package

    #### Unmanaged mode

    $ hadoop jar $HADOOP_YARN_HOME/share/hadoop/yarn/hadoop-yarn-applications-unmanaged-am-launcher-2.1.1-SNAPSHOT.jar YarnAppClient -classpath your_jar.jar -cmd "java com.bigschool.yarn.YarnAppMaster /bin/date 2"

    #### Managed mode

    $ bin/hadoop fs -put target/your_jar.jar /apps/

    $ hadoop jar your_jar.jar com.bigschool.yarn.YarnAppClient /bin/date 2 /apps/your_jar.jar

    OR
    $ sh runner.sh

Browse http://localhost:8088 for job status

REFERENCES:
-----------
- https://github.com/DemandCube/yarn-app
- http://tzulitai.wordpress.com/2013/09/04/applicationmaster-yarn-applications-code-level-breakdown
- http://tzulitai.wordpress.com/2013/08/30/yarn-applications-code-level-breakdown-client
- hadoop.apache.org/docs/r2.3.0/hadoop-yarn/hadoop-yarn-site/WritingYarnApplications.html
