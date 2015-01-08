package com.bigschool;

import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;

import java.util.Properties;

/**
 *  S3 Native FileSystem (URI scheme: s3n)
 *  --------------------------------------
 *  A native filesystem for reading and writing regular files on s3.
 *  The advantage of this filesystem is that you can access files on s3
 *  that were written with other tools. Conversely, other tools can access
 *  files written using Hadoop. The disadvantage is the 5GB limit on file size
 *  imposed by s3. For this reason it is not suitable as a replacement for HDFS
 *  (which has support for very large files).
 *
 *  S3 Block FileSystem (URI scheme: s3)
 *  ------------------------------------
 *  A block-based filesystem backed by s3.
 *  Files are stored as blocks, just like they are in HDFS.
 *  This permits efficient implementation of renames.
 *  This filesystem requires you to dedicate a bucket for the filesystem - you should not use
 *  an existing bucket containing files, or write other files to the same bucket.
 *  The files stored by this filesystem can be larger than 5GB, but they are not
 *  interoperable with other s3 tools.
 *
 *
 *  Note
 *  ------
 *  Note that for input you must create bucket manually and upload files in that bucket,
 *  for output you must create bucket and directory specified for output must not already exist.
 *
 *  Configuration for S3
 *  ---------------------
 *   Now add the following entry to hdfs-site.xml
 *   $ vim HADOOP_HOME/etc/hadoop/hdfs-site.xml
         <property>
             <name>fs.s3.awsAccessKeyId</name>
             <value>AWS-ACCESS-KEY</value>
         </property>
         <property>
             <name>fs.s3.awsSecretAccessKey</name>
             <value>AWS-SECRET-KEY</value>
         </property>

         OR
        You can supply these properties to FlowConnector as done in example below.
         OR
        s3n://ID:SECRET@BUCKET (use as url)
        Example:
        ===========
        $> HADOOP_HOME/bin/hadoop distcp hdfs://ip:9001/user/nutch/file.csv s3://ACCESSKEY:SECRETKEY@bucket_name/
 *
 *
 * What are Access Key and Secret Key
 * ----------------------------------
 *  AWS access key
 *  ==============
 *  This is actually a  username .
 *  It is alphanumeric text string that uniquely identifies the user who owns the account.
 *  No two accounts can have the same AWS Access Key.
 *
 *  AWS Secret key
 *  ===============
 *  This key plays the role of a  password .
 *  It's called secret because it is assumed to be known by the owner only that's why,
 *  when you type it in the given box, its displayed as asterisk or dots.
 *  A Password with Access Key forms a secure information set that confirms the user's identity.
 *  You are advised to keep your Secret Key in a safe place.
 *
 *  Disadvantages
 *  --------------------
 *  Note that by using S3 as an input you lose the data locality optimization,
 *  which may be significant. The general best practise is to copy in data using distcp
 *  at the start of a workflow, then copy it out at the end, using the transient HDFS in between.
 *
 */
public class Main {

    public void run(String[] args) {
        Properties properties = new Properties();
        String accessKey = args[0];
        String secretKey = args[1];

        // better put these keys to hadoop xml file
        properties.setProperty("fs.s3.awsAccessKeyId", accessKey);
        properties.setProperty("fs.s3.awsSecretAccessKey", secretKey);

        // properties.setProperty("fs.s3n.awsAccessKeyId", accessKey);
        // properties.setProperty("fs.s3n.awsSecretAccessKey", secretKey);

        // properties.setProperty("fs.defaultFS", "hdfs://localhost:8020/");
        // properties.setProperty("fs.permissions.umask-mode", "007");

        AppProps.setApplicationJarClass(properties, Main.class);

        HadoopFlowConnector flowConnector = new HadoopFlowConnector( properties );

        String input = "s3://my-bucket/my-log.csv";

        Tap inTap = new Hfs( new TextDelimited( false, ";" ), input);

        Pipe copyPipe = new Pipe( "copy" );
        Tap outTap = new Hfs( new TextDelimited( false, ";" ), "data/output");

        FlowDef flowDef = FlowDef.flowDef()
                .addSource( copyPipe, inTap )
                .addTailSink( copyPipe, outTap );

        flowConnector.connect( flowDef ).complete();
    }

    public static void main(String[] args) {
        new Main().run(args);
    }
}
