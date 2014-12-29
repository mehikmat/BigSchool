package com.bigschool.filter;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class RowFilterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] row = value.toString().split(";");

        // remove rows if they contains string "ram" at first position
        if(!row[0].equalsIgnoreCase("ram")) {
            for (String field : row) {
                if (!value.toString().equalsIgnoreCase("ram")) {
                    context.write(new Text(field), new IntWritable(1));
                }
            }
        }
    }
}
