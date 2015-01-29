package com.bigschool;

import com.bigschool.mapper.StudentInfoMapper;
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
        StudentInfoMapper mapper = new StudentInfoMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void testMapper(){
        // data layout
        //#student_first_name;student_middle_name;student_last_name;student_address;student_phone;student_roll;student_marks
        mapDriver
        .withInput(new LongWritable(), new Text("Demetrius Vinson;Sanchez;Stephens;Ap #791-1271 Vivamus St.;07624 661704;4669845150597397;5"))
        // mapDriver.withOutput(new Text("Demetrius Vinson"), new IntWritable(1));
        .withOutput(new Pair<Text, IntWritable>(new Text("Demetrius Vinson"),new IntWritable(5)))
        .runTest();
    }

    @Test
     public void testMultipleOutputs() throws IOException {
        //TODO: test with multiple input records

        mapDriver.withInput(new LongWritable(2), new Text("Hikmat Vinson;Sanchez;Stephens;Ap #791-1271 Vivamus St.;07624 661704;4669845150597397;5"));
        mapDriver.withInput(new LongWritable(1), new Text("Demetrius Vinson;Sanchez;Stephens;Ap #791-1271 Vivamus St.;07624 661704;4669845150597397;5"));
        final List<Pair<Text, IntWritable>> result = mapDriver.run();

        final Pair<Text, IntWritable> r1 = new Pair<Text, IntWritable>(new Text("Demetrius Vinson"), new IntWritable(5));
        //final Pair<Text, IntWritable> r2 = new Pair<Text, IntWritable>(new Text("Hikmat Vinson"), new IntWritable(5));

       Assertions.assertThat(result)
               .isNotNull()
               .hasSize(1)
               .contains(r1/*r2*/);
    }
}
