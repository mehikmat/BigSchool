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

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: $HADOOP_HOME/bin/yarn jar [input] [output]");
            System.exit(-1);
        }

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

        // submit job to cluster
        job.submit();
    }
}
