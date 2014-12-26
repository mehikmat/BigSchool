package com.bigschool;

import com.bigschool.mapper.BigSchoolMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.fest.assertions.Assertions;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class BigSchoolMapperTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

    @Before
    public void setUp() {
        BigSchoolMapper mapper = new BigSchoolMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper(){
        mapDriver
        .withInput(new LongWritable(), new Text("hikmat"))
        // mapDriver.withOutput(new Text("hikmat"), new IntWritable(1));
        .withOutput(new Pair<Text, IntWritable>(new Text("hikmat"),new IntWritable(1)))
        .runTest();
    }

    @Test
     public void testMultipleOutputs() throws IOException {
        mapDriver.withInput(new LongWritable(), new Text("hikmat singh dhamee hikmat singh dhamee"));
        final List<Pair<Text, IntWritable>> result = mapDriver.run();

        final Pair<Text, IntWritable> r1 = new Pair<Text, IntWritable>(new Text("hikmat"), new IntWritable(1));
        final Pair<Text, IntWritable> r2 = new Pair<Text, IntWritable>(new Text("singh"), new IntWritable(1));

       Assertions.assertThat(result)
               .isNotNull()
               .hasSize(6)
               .contains(r1, r2);
    }
}
