package com.bigschool.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: hikmat
 * Date: 3/15/13
 * Time: 12:59 PM
 * To change this template use File | Settings | File Templates.
 */

public class BigSchoolMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        //super.map(key,value,context);
        StringTokenizer st = new StringTokenizer(value.toString().toLowerCase());

        while(st.hasMoreTokens()) {
            context.write(new Text(st.nextToken()), new IntWritable(1));
        }
    }
}
