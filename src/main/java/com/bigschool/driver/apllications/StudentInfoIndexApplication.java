package com.bigschool.driver.apllications;

import com.bigschool.mapper.StudentIndexMapper;
import com.bigschool.partitioner.BigSchoolPartitioner;
import com.bigschool.util.ResourceUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Date;
import java.util.Properties;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class StudentInfoIndexApplication implements HadoopApplication {
    private static final String CLUSTER_CONFIG = "es_cluster_config.properties";

    @Override
    public int runApplication(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = Job.getInstance(getConfiguration());

        // set key types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set mapper
        job.setMapperClass(StudentIndexMapper.class);

        // set combiner // for now we don't need reducer to index to es
        //job.setCombinerClass(IdentityReducer.class);

        // set partitioner
        job.setPartitionerClass(BigSchoolPartitioner.class);

        // set reducer // for now we don't need reducer to index to es
        //job.setReducerClass(IdentityReducer.class);

        //for sequence file input
        //job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);

        // for sequence file output
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // for sequence file input path
        //SequenceFileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.setInputPaths(job, new Path(args[1]));

        // for sequence file output path
        //SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setJarByClass(StudentInfoProcessApplication.class);
        job.setJobName("MRv2-Index-Student-Info");
        // Define the comparator that controls how the keys are sorted before they
        // are passed to the Reducer
        //job.setSortComparatorClass(null);

        // Define the comparator that controls which keys are grouped together
        // for a single call to reduce method
        // job.setGroupingComparatorClass(null);

        // record start time
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);

        // Submit the job to the cluster and return immediately.
        // Do when the multiple jobs need to be submitted together.
        //job.submit();

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
        Properties properties = ResourceUtils.loadProperties(CLUSTER_CONFIG);
        for (String key : properties.stringPropertyNames()){
            configuration.set(key,properties.getProperty(key));
        }
        // setup custom job configs
        configuration.set("index_name","demo");
        configuration.set("","demo");
        return configuration;
    }
}
