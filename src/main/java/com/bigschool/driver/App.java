package com.bigschool.driver;

import com.bigschool.mapper.BigSchoolMapper;
import com.bigschool.reducer.BigSchoolReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * Created with IntelliJ IDEA.
 * User: hikmat
 * Date: 3/15/13
 * Time: 1:05 PM
 * To change this template use File | Settings | File Templates.
 */
public class App {

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: $HADOOP_HOME/bin/yarn jar [input] [output]");
            System.exit(-1);
        }

        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(BigSchoolMapper.class);
        job.setReducerClass(BigSchoolReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(App.class);
        job.setJobName("MRv2-WordCount");

        job.submit();
    }
}
