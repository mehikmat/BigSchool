package com.bigschool.sensordataprocessing;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * 3 types of joins:
 * 1. Reduce-Side joins (easiest),
 * 2. Map-Side joins and
 * 3. Memory-Backed Join
 * For this we need
 * Composite key
 * Composite key comparator(for secondary sorting)
 * Natural key partitioner
 * Natural key grouping comparator
 * Mapper
 * Reducer
 * <p/>
 * http://codingjunkie.net/mapreduce-reduce-joins/
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class SensorDataProcessorDriver {
    /*  #########Join hvac.csv and buildings.csv data
        create table if not exists hvac_buildings as select h.*, b.* from buildings as b join hvac as h on h.buildingid=b.buildingid;

        ########Analytics/refine
        CREATE TABLE hvac_report as
        select *,
        targettemp - actualtemp as temp_diff,
        IF((targettemp - actualtemp) > 5, 'COLD',IF((targettemp - actualtemp) < -5, 'HOT', 'NORMAL')) AS temprange,
        IF((targettemp - actualtemp) > 5, '2',IF((targettemp - actualtemp) < -5, '1',0)) AS extremetemp
        from hvac_buildings;
    */
    public static void main(String[] args) throws Exception {
        String[] joinInputFiles = new String[]{"input/buildings.csv","input/hvac.csv"}; // input files
        String joinOutputFile = "hvac_buildings"; // output file: after join
        String analysisOutputFile = "hvac_buildings_report"; // output file: after join

        Splitter splitter = Splitter.on('/'); // file path separator
        StringBuilder filePaths = new StringBuilder();

        Configuration config = new Configuration();
        config.set("keyIndex", "0"); // natural key position among all fields
        config.set("separator", ","); // field separator

        for (int i = 0; i < joinInputFiles.length; i++) {
            String fileName = Iterables.getLast(splitter.split(joinInputFiles[i]));
            config.set(fileName, Integer.toString(i + 1)); // set file join order
            filePaths.append(joinInputFiles[i]).append(",");// make a string containing all input files separated by comma
        }

        filePaths.setLength(filePaths.length() - 1);


        // first job
        Job joinJob = Job.getInstance(config);
        joinJob.setJobName("Join_Job");
        joinJob.setJarByClass(SensorDataProcessorDriver.class);

        FileInputFormat.addInputPaths(joinJob, filePaths.toString());
        FileOutputFormat.setOutputPath(joinJob, new Path(joinOutputFile));

        joinJob.setInputFormatClass(TextInputFormat.class);
        joinJob.setOutputFormatClass(TextOutputFormat.class);

        joinJob.setMapperClass(JoiningMapper.class);
        joinJob.setReducerClass(JoiningReducer.class);

        joinJob.setPartitionerClass(TaggedKeyPartitioner.class);
        joinJob.setGroupingComparatorClass(TaggedKeyGroupingComparator.class);

        joinJob.setOutputKeyClass(TaggedKey.class);
        joinJob.setOutputValueClass(Text.class);


        //second job
        Job analysisJob = Job.getInstance();
        analysisJob.setJobName("Analysis_Job");
        analysisJob.setJarByClass(SensorDataProcessorDriver.class);

        FileInputFormat.addInputPaths(analysisJob, joinOutputFile);
        FileOutputFormat.setOutputPath(analysisJob, new Path(analysisOutputFile));

        analysisJob.setInputFormatClass(TextInputFormat.class);
        analysisJob.setOutputFormatClass(TextOutputFormat.class);

        analysisJob.setOutputKeyClass(TaggedKey.class);
        analysisJob.setOutputValueClass(Text.class);


        // specify job controllers
        ControlledJob joinJobController = new ControlledJob(config);
        joinJobController.setJob(joinJob);

        ControlledJob analysisJobController = new ControlledJob(config);
        analysisJobController.setJob(analysisJob);

        //Using the jobControl object, you specify the job dependencies and then run the jobs:
        JobControl joinChain = new JobControl("JobChain");

        joinChain.addJob(joinJobController);
        joinChain.addJob(analysisJobController);

        // specify dependency
        analysisJobController.addDependingJob(joinJobController);

        // execute the whole job chain
        joinChain.run();
    }
}
