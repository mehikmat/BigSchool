package com.bigschool;

import com.bigschool.comparator.CustomKeyOrderComparator;
import com.bigschool.filter.RowFilterMapper;
import com.bigschool.mapper.StudentInfoMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Operations Tested Here
 * ======================
 *  1. Sorting
 *  2. Filtering
 *
 */
public class MapReduceOperationTest {
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() {
        StudentInfoMapper mapper = new StudentInfoMapper();
        mapReduceDriver = MapReduceDriver.newMapReduceDriver();

        /**
         * New MapReduce API provides Mapper and Reducer instead of IdentityMapper and IdentityReducer
         * So for just sorting use these classes
         * These classes don't perform any operation but work just as data migration object
         * They transfer data as it was.
         */
        Reducer<Text, IntWritable, Text, IntWritable> reducer = new Reducer<Text, IntWritable, Text, IntWritable>();


        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);

        mapReduceDriver.setKeyGroupingComparator( new CustomKeyOrderComparator());

        mapReduceDriver.setKeyOrderComparator(new CustomKeyOrderComparator());
    }

    // Test sorting by framework
    // Use identity mapper and identity reduce for sorting
    @SuppressWarnings("unchecked")
    @Test
    public void testSorting() throws IOException {
        mapReduceDriver.withInput(new LongWritable(), new Text("Hikmat;student_middle_name;student_last_name;student_address;student_phone;student_roll;5"));

        final List<Pair<Text, IntWritable>> result = mapReduceDriver.run();

        System.out.println("================SORTED OUTPUT===============");

        for (Pair<Text,IntWritable> k2v2 : result){
            System.out.println(k2v2.toString());
        }

        System.out.println("================SORTED OUTPUT===============");

    }

    // Test filtering by mapper
    @SuppressWarnings("unchecked")
    @Test
    public void testFiltering() throws IOException {
        //set filter mapper
        mapReduceDriver.setMapper(new RowFilterMapper());

        mapReduceDriver.withInput(new LongWritable(), new Text("ram;singh;dhamee")); // row1
        mapReduceDriver.withInput(new LongWritable(), new Text("ram;singh;dhamee")); // row2
        mapReduceDriver.withInput(new LongWritable(), new Text("hari;singh;dhamee")); // row3


        final List<Pair<Text, IntWritable>> result = mapReduceDriver.run();

        System.out.println("================SORTED OUTPUT===============");

        for (Pair<Text,IntWritable> k2v2 : result){
            System.out.println(k2v2.toString());
        }

        System.out.println("================SORTED OUTPUT===============");

    }
}
