package com.bigschool.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class StudentInfoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] row = value.toString().split(";");

        if (!value.toString().startsWith("#")) {
            // write student_name and corresponding mark i.e group by student name
            context.write(new Text(row[0]), new IntWritable(Integer.parseInt(row[row.length - 1])));
        }
    }
}
