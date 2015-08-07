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

    $ git checkout redis_cache

    $ mvn clean package

    $ sh runner.sh

  In case you get an error of type "Not valid JAR", check the jar path. It might be different from one plateform to another with mvn.


Browse http://localhost:8088 for job status

What is Redis?
==============
Redis is an open source, BSD licensed, advanced key-value cache and store.
It is often referred to as a data structure server since keys can contain strings,
hashes, lists, sets, sorted sets, bitmaps and hyperloglogs.

It can be use for checking existence against large data set stored in HDFS or somewhere else.
It's very fast than Hadoop Distributed Cache.

REFERENCES:
-----------
- https://github.com/xetorthio/jedis [Redis Java Client Library]
- http://redis.io/