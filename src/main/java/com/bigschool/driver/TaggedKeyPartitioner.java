package com.bigschool.driver;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by hdhamee on 4/12/16.
 */
public class TaggedKeyPartitioner extends Partitioner<TaggedKey,Text> {

    @Override
    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        return taggedKey.getJoinKey().hashCode() % numPartitions;
    }
}