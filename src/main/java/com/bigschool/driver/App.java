package com.bigschool.driver;

import com.bigschool.mapper.BigSchoolMapper;
import com.bigschool.partitioner.BigSchoolPartitioner;
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

import java.util.Date;

/**
 * MapReduce Operations showcased here are
 *
 * ===============================================
 *  MapReduce Operators
 *  --------------------
 *  1. Mapper
 *  2. Reducer
 *  3. Combiner
 *  4. Partitioner
 *  5. Sorting
 *
 *  Common MapReduce Patterns
 *  -------------------------
 *  1. Filtering or Grepping
 *  2. Parsing, Conversion
 *  3. Counting, Summing
 *  4. Binning, Collating
 *  5. Distributed Tasks
 *  6. Simple Total Sorting
 *  7. Chained Jobs
 *  ==============================================
 *
 *  Combiner
 *  --------
 *   The best part of all is that we do not need to write any additional code
 *   to take advantage of this! If a reduce function is both commutative
 *   and associative, then it can be used as a Combiner as well.
 *
 *   Partitioner
 *   -----------
 *   The key and value are the intermediate key and value produced by the map function.
 *   The numReduceTasks is the number of reducers used in the MapReduce program
 *   and it is specified in the driver program.
 *   It is possible to have empty partitions with no data (when no of partition is less then no of reducer).
 *   We do the assigned partition number modulo numReduceTasks to avoid illegal partitions
 *   if the system has a lesser number of possible reducers than the assigned partition number.
 *
 *   The partitioning phase takes place after the map/combine phase and before the reduce phase.
 *   The number of partitions is equal to the number of reducers.
 *   The data gets partitioned across the reducers according to the partitioning function
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class App {

    public static int runJob(String[] args) throws Exception{
        Job job = Job.getInstance(new Configuration());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set mapper
        job.setMapperClass(BigSchoolMapper.class);

        // set combiner
        job.setCombinerClass(BigSchoolReducer.class);

        // set partitioner
        job.setPartitionerClass(BigSchoolPartitioner.class);

        // set reducer
        job.setReducerClass(BigSchoolReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setJarByClass(App.class);
        job.setJobName("MRv2-WordCount");

        /**
         * http://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
         */
        // Define the comparator that controls how the keys are sorted before they
        // are passed to the Reducer
        job.setSortComparatorClass(null);

        /**
         * Reducer Instance vs reduce method:
         * One JVM is created per Reduce task and each of these has a single instance
         * of the Reducer class.This is Reducer instance(I call it Reducer from now).
         * Within each Reducer, reduce method is called multiple times depending on
         * 'key grouping'.Each time reduce is called, 'valuein' has a list of map output
         * values grouped by the key you define in 'grouping comparator'.By default,
         * grouping comparator uses the entire map output key.
         *
         * Example*
         * Input:*
          symbol time price
             a 1 10
             a 2 20
             b 3 30
         Map output: create composite key\values like so symbol-time time-price
             a-1 1-10
             a-2 2-20
             b-3 3-30
         The Partitioner: will route the a-1 and a-2 keys to the same reducer despite the keys being different.
         It will also route the b-3 to a separate reducer.

         GroupComparator: once the composites key\value arrive at the reducer instead of the reducer getting
             (a-1,{1-10})
             (a-2,{2-20})
         the above will happen due to the unique key values following composition.
         the group comparator will ensure the reducer gets:
             (a,{1-10,2-20})
         [[In a single reduce method call.]]
         */
        // Define the comparator that controls which keys are grouped together
        // for a single call to reduce method
        job.setGroupingComparatorClass(null);

        // record start time
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);

        // submit job to cluster
        job.submit();

        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took " +
                (endTime.getTime() - startTime.getTime()) /1000 + " seconds.");

        // return status
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length >= 2) {
            System.out.println("Usage: $HADOOP_HOME/bin/yarn jar [input] [output]");
            System.exit(-1);
        }
        System.exit(runJob(args));
    }
}
