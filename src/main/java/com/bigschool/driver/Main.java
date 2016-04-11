package com.bigschool.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * 3 types of joins:
 *  Reduce-Side joins (easiest),
 *  Map-Side joins and
 *  Memory-Backed Join
 *For this we need
 *  Composite key
 *  Composite key comparator(for secondary sorting)
 *  Natural key partitioner
 *  Natural key grouping comparator
 *  Mapper
 *  Reducer
 *
 *  http://codingjunkie.net/mapreduce-reduce-joins/
 *
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // set key types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setJarByClass(Main.class);
        job.setJobName("MRv2-Process");

        //Submit the job to the cluster and wait for it to finish
        job.waitForCompletion(true);
    }
}
