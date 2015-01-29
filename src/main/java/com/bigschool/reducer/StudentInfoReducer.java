package com.bigschool.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class StudentInfoReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalMarks = 0;

        // sum up the marks of a student
        for (IntWritable value : values) {
            totalMarks += value.get();
        }
        // write student_name as key and corresponding total marks as value
        context.write(key, new IntWritable(totalMarks));
    }
}
