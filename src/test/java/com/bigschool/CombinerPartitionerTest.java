package com.bigschool;

import com.bigschool.mapper.BigSchoolMapper;
import com.bigschool.reducer.BigSchoolReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
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
public class CombinerPartitionerTest {

    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapCombinerDriver;

    @Before
    public void setUp(){
        BigSchoolMapper mapper = new BigSchoolMapper();
        BigSchoolReducer combiner = new BigSchoolReducer();

        mapCombinerDriver = MapReduceDriver.newMapReduceDriver();

        mapCombinerDriver.setMapper(mapper);
        mapCombinerDriver.setCombiner(combiner);
        mapCombinerDriver.setReducer(combiner);

       // mapCombinerDriver = MapReduceDriver.newMapReduceDriver(mapper, combiner);
    }

    @Test
    public void testMultipleOutputs() throws IOException {
        mapCombinerDriver.withInput(new LongWritable(), new Text("hikmat singh dhamee hikmat singh dhamee"));
        final List<Pair<Text, IntWritable>> result = mapCombinerDriver.run();

        System.out.println("OUTPUT>> " + result.toString());

        final Pair<Text, IntWritable> r1 = new Pair<Text, IntWritable>(new Text("hikmat"), new IntWritable(2));

        Assertions.assertThat(result)
                .isNotNull()
                .hasSize(3)
                .contains(r1);
    }
}
