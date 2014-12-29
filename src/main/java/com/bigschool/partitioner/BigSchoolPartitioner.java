package com.bigschool.partitioner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Partitioner determiners which record belongs to which reducer
 * By default (hash based partitioner), same key records belong to a reducer
 * but by using custom partitioner we can send different key records to a reducer.
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class BigSchoolPartitioner extends Partitioner<Text,IntWritable> {

    // key and value are the output from map task
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {

        int age = value.get();

        //this is done to avoid performing mod with 0
        if(numReduceTasks == 0)
            return 0;

        //if the age is <20, assign partition 0
        if(age <= 20){
            return 0;
        }
        //else if the age is between 20 and 50, assign partition 1
        if(age > 20 && age <=50){

            return 1 % numReduceTasks;
        }
        //otherwise assign partition 2
        else
            return 2 % numReduceTasks;
    }
}

