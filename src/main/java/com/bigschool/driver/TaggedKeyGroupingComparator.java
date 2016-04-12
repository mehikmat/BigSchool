package com.bigschool.driver;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by hdhamee on 4/12/16.
 */
public class TaggedKeyGroupingComparator extends WritableComparator {


    public TaggedKeyGroupingComparator() {
        super(TaggedKey.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TaggedKey taggedKey1 = (TaggedKey)a;
        TaggedKey taggedKey2 = (TaggedKey)b;
        return taggedKey1.getJoinKey().compareTo(taggedKey2.getJoinKey());
    }
}