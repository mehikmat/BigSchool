package com.bigschool.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Date;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        Main main = new Main();
        main.runApplication(args);
    }

    public int runApplication(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConfiguration());

        // set key types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // set mapper
        job.setMapperClass(Mapper.class);

        //set reducer
        job.setReducerClass(Reducer.class);

        //for sequence file input
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(PDFFileInputFormat.class);

        // for sequence file output
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // for sequence file input path
        //SequenceFileInputFormat.setInputPaths(job,new Path(args[0]));
        PDFFileInputFormat.setInputPaths(job, new Path(args[1]));

        // for sequence file output path
        //SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setJarByClass(Main.class);
        job.setJobName("Custom_PDF_Input_Format_Test");

        // record start time
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);

        //Submit the job to the cluster and wait for it to finish
        job.waitForCompletion(true);

        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took " +
                (endTime.getTime() - startTime.getTime()) /1000 + " seconds.");

        // return status
        return 0;
    }

    private Configuration getConfiguration(){
        Configuration configuration = new Configuration();
        //configuration.set("textinputformat.record.delimiter", "\n");
        return configuration;
    }
}
