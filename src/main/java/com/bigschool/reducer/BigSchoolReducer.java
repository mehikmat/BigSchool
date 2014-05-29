package com.bigschool.reducer;

/**
 * Created with IntelliJ IDEA.
 * User: hikmat
 * Date: 3/15/13
 * Time: 1:04 PM
 * To change this template use File | Settings | File Templates.
 */


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;


public class BigSchoolReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
        int count = 0;
        Iterator<IntWritable> it = values.iterator();

        while (it.hasNext()) {
            count += it.next().get();
        }
        context.write(key, new IntWritable(count));
    }
}
