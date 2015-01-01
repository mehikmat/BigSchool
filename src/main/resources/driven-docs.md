What is Driven
--------------
Driven is a client-server based application for
Performance Management and Operational Visualization
for Cascading Applications.

Driven consists of two components
---------------------------------
- Driven Server: is a web-application that is hosted in
web server.

- Driven Client (Driven Plugin): is the java application
which is configured with Cascading Application to be monitored.

NOTE:
------
You do not need to make any changes to existing
Cascading applications to integrate with the Driven system.
Driven supports Cascadinng based all applications including
Scalding, Cascalog, Lingual, Pattern), Apache Hive, and native MapReduce.

How to setup Driven for cascading application
---------------------------------------------
Driven can be set up in two ways depending on your requirement.
- Driven For Enterprise
  In this way one can deploy driven server anywhere on their own
  environment.
  Core Features
    Application Timeline
    Application Runtime Visibility
    Team Collaboration & Sharing
    Complete History Retention
    Rich Search
    Powered by a meta-data repo
  Enterprise Features
    Securely host in your own environment
    Integration with Third-Party Monitoring Tools
    Command-line Interface

Sing up to http://cascading.io/trial/ for trial and you will be provided
with a 30-day trial license key and can download Driven to host
in your own environment.

- Driven Early Access
  In this way one cannot deploy driven server anywhere on their own
  environment and need to use cascading.io shosted driven server.

  All the core features are available but Enterprise features are not
  available

Set up driven plugin
--------------------
Settin up driven plugin is same for above both ways.
Add the repository
<repository>
  <id>conjars</id>
  <url>http://conjars.org/repo</url>
</repository>
Add driven plugin dependency
<dependency>
   <groupId>driven</groupId>
   <artifactId>driven-plugin</artifactId>
   <version>1.2-eap-4</version>
</dependency>







