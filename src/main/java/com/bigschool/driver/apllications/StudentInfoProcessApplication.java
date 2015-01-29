package com.bigschool.driver.apllications;

import com.bigschool.mapper.StudentInfoMapper;
import com.bigschool.partitioner.BigSchoolPartitioner;
import com.bigschool.reducer.StudentInfoReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Date;

/**
 * Sorting Comparators
 * ==================
 * Useful link for secondary sort implementation by means of composite primary key {natural_key,secondary_key}
 * http://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
 * Steps:
 *   1. Write custom partitioner and use natural key for partitioning (group by key)
 *      (multiple unique keys may go to same reducer). So
 *   2. Write custom natural key grouping comparator (primary sort)
 *   3. Write custom composite key comparator (secondary sort)
 *        int result = k1.getPrimaryKey().compareTo(k2.getPrimaryKey());
 *        if(0 == result) {
 *        result = -1* k1.getSecondaryKey().compareTo(k2.getSecondaryKey());
 *        }
 *
 * Grouping Comparator
 * ===================
 * Reducer Instance vs reduce method:
 * One JVM is created per Reduce task and each of these has a single instance
 * of the Reducer class.This is Reducer instance(I call it Reducer from now).
 * Within each Reducer, reduce method is called multiple times depending on
 * 'key grouping'.Each time reduce is called, 'valuein' has a list of map output
 * values grouped by the key you define in 'grouping comparator'.By default,
 * grouping comparator uses the entire map output key.
 *
 * Example
 * Input:
 * symbol time price
 *    a 1 10
 *    a 2 20
 *    b 3 30
 * Map output: create composite key\values like so symbol-time time-price
 *    a-1 1-10
 *    a-2 2-20
 *    b-3 3-30
 * The Partitioner: will route the a-1 and a-2 keys to the same reducer despite the keys being different.
 * It will also route the b-3 to a separate reducer.
 *
 * GroupComparator: once the composites key\value arrive at the reducer instead of the reducer getting
 *    (a-1,{1-10})
 *    (a-2,{2-20})
 * the above will happen due to the unique key values following composition.
 * the group comparator will ensure the reducer gets:
 *    (a,{1-10,2-20})
 * [[In a single reduce method call.]]
 *
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class StudentInfoProcessApplication implements HadoopApplication {

    @Override
    public int runApplication(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // set key types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set mapper
        job.setMapperClass(StudentInfoMapper.class);

        // set combiner
        job.setCombinerClass(StudentInfoReducer.class);

        // set partitioner
        job.setPartitionerClass(BigSchoolPartitioner.class);

        // set reducer
        job.setReducerClass(StudentInfoReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        //for sequence file input
        //job.setInputFormatClass(SequenceFileInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // for sequence file output
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        // for sequence file input path
        //SequenceFileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        // for sequence file output path
        //SequenceFileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(StudentInfoProcessApplication.class);
        job.setJobName("MRv2-Process-Student-Info");
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
        System.out.println("The job took=====================>>>>>> " +
                (endTime.getTime() - startTime.getTime()) /1000 + " seconds.");

        // return status
        return 0;
    }
}
