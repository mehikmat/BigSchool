package com.bigschool;

import com.bigschool.mapper.BigSchoolMapper;
import com.bigschool.reducer.BigSchoolReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class BigSchoolReduceTest {
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        BigSchoolReducer reducer = new BigSchoolReducer();
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
    }
    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("hikmat"), values);
        reduceDriver.withOutput(new Text("hikmat"), new IntWritable(3));
        reduceDriver.runTest();
    }

    @Test
    public void testMultipleOutputs() throws IOException {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));

        reduceDriver.withInput(new Text("hikmat"), values);

        final List<Pair<Text, IntWritable>> result = reduceDriver.run();

        final Pair<Text, IntWritable> r1 = new Pair<Text, IntWritable>(new Text("hikmat"), new IntWritable(3));

        Assertions.assertThat(result)
                .isNotNull()
                .hasSize(1)
                .contains(r1);
    }
}
