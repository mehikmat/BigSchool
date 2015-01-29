package com.bigschool;

import com.bigschool.mapper.StudentInfoMapper;
import com.bigschool.reducer.StudentInfoReducer;
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
        StudentInfoMapper mapper = new StudentInfoMapper();
        StudentInfoReducer combiner = new StudentInfoReducer();

        mapCombinerDriver = MapReduceDriver.newMapReduceDriver();

        mapCombinerDriver.setMapper(mapper);
        mapCombinerDriver.setCombiner(combiner);
        mapCombinerDriver.setReducer(combiner);

       // mapCombinerDriver = MapReduceDriver.newMapReduceDriver(mapper, combiner);
    }

    @Test
    public void testMultipleOutputs() throws IOException {
        //TODO: test with multiple input records

        mapCombinerDriver.withInput(new LongWritable(), new Text("Hikmat Vinson;Sanchez;Stephens;Ap #791-1271 Vivamus St.;07624 661704;4669845150597397;5"));
        final List<Pair<Text, IntWritable>> result = mapCombinerDriver.run();

        System.out.println("OUTPUT>> " + result.toString());

        final Pair<Text, IntWritable> r1 = new Pair<Text, IntWritable>(new Text("Hikmat Vinson"), new IntWritable(5));

        Assertions.assertThat(result)
                .isNotNull()
                .hasSize(1)
                .contains(r1);
    }
}
