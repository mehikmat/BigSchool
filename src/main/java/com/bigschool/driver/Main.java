package com.bigschool.driver;

import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

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
        Splitter splitter = Splitter.on('/');
        StringBuilder filePaths = new StringBuilder();

        Configuration config = new Configuration();
        config.set("keyIndex", "0");
        config.set("separator", ",");

        for(int i = 0; i< args.length - 1; i++) {
            String fileName = Iterables.getLast(splitter.split(args[i]));
            config.set(fileName, Integer.toString(i+1));
            filePaths.append(args[i]).append(",");
        }

        filePaths.setLength(filePaths.length() - 1);
        Job job = Job.getInstance(config, "ReduceSideJoin");
        job.setJarByClass(Main.class);

        FileInputFormat.addInputPaths(job, filePaths.toString());
        FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

        job.setMapperClass(JoiningMapper.class);
        job.setReducerClass(JoiningReducer.class);
        job.setPartitionerClass(TaggedKeyPartitioner.class);
        job.setGroupingComparatorClass(TaggedKeyGroupingComparator.class);
        job.setOutputKeyClass(TaggedKey.class);
        job.setOutputValueClass(Text.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
