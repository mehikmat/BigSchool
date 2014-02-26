package com.bigschool.mapper;

/**
 * Created with IntelliJ IDEA.
 * User: hikmat
 * Date: 3/15/13
 * Time: 12:59 PM
 * To change this template use File | Settings | File Templates.
 */


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.StringTokenizer;

public class BigSchoolMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
        StringTokenizer st = new StringTokenizer(value.toString().toLowerCase());
        while(st.hasMoreTokens()) {
            output.collect(new Text(st.nextToken()), new IntWritable(1));
        }
    }
}
