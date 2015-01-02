What is Driven
------------------
Driven is a client-server based application for Performance Management
and Operational Visualization for Cascading Based Applications.

Driven consists of two components
---------------------------------
- *Driven Server:* is a web-application that is hosted in web server.
- *Driven Client (Driven Plugin):* is the java application which is configured with Cascading Applications to be monitored.

NOTE:
------
- You do not need to make any changes to existing Cascading applications to integrate with the Driven system.
- Driven supports Cascading based all applications including
Scalding, Cascalog, Lingual, Pattern), Apache Hive, and native MapReduce.

How to setup Driven for cascading application
---------------------------------------------
Driven can be set up in two ways depending on your requirement.

####Driven For Enterprise####
In this way one can deploy driven server anywhere on their own environment.
 
_Core features available_

    - Application Timeline
    - Application Runtime Visibility
    - Team Collaboration & Sharing
    - Complete History Retention
    - Rich Search
    - Powered by a meta-data repo
    
_Enterprise features available_

    _ Securely host in your own environment
    _ Integration with Third-Party Monitoring Tools
    _ Command-line Interface

if you want to test driven features (Don't have money to buy license)

- Sing up to http://cascading.io/trial/ for trial and you will be provided
with a 30-day trial license key and can download Driven to host
in your own environment.

- Unzip the driven-tomcat package and start

- $> DRIVEN_HOME/bin/startup.sh

After starting the server go to http://<hostname>:8080.
There you will be presented with a form to enter license information
needed to complete the Driven server installation.

Login with username:admin and password:admin (default)
and an API key will also be provided which should be used with drive plugin.

Customize driven server by changing default configuration located at
SERVER_HOME/conf/driven.properties


####Driven Early Access####
In this way one cannot deploy driven server anywhere on their own
environment and need to use concurrent (driven.cascading.io) hosted driven server.

All the core features are available but Enterprise features are not available.

In this cas you need to login using your credentials and then an API key will be  provided to use with driven plugin.

Set up driven plugin
--------------------
Settin up driven plugin is similar for above both ways.

Add the repository
```
<repository>
  <id>conjars</id>
  <url>http://conjars.org/repo</url>
</repository>
```

Add driven plugin dependency
```
<dependency>
   <groupId>driven</groupId>
   <artifactId>driven-plugin</artifactId>
   <version>1.2-eap-4</version>
    <!--if you want use concurrent hosted driven server 
    <classifier>io</classifier>-->
</dependency>
```
**If you are running your application on Hadoop**

- Create a file named cascading-service.properties and put the following property
```
cascading.management.document.service.apikey = YOUR_API_KEY
cascading.management.document.service.hosts = YOUR_DRIVEN_SERVER_URL
(Don't specify host property if are using hosted driven)
```
- Copy this file inside hadoop conf dir. That's all.

**If you are running your application in Local Mode**

Export the following properties to your environment
```
export DRIVEN_API_KEY=_YOUR_API_KEY_
export DRIVEN_SERVER_HOSTS=_YOUR_DRIVEN_SERVER_URL_
(don't export host property if you are using hosted driven)
```

Creating Tags:
---------------
http://docs.cascading.io/driven/1.2/user-guide/tags.html

Creating Teams and users:
---------------------------
http://docs.cascading.io/driven/1.2/user-guide/teams.html











