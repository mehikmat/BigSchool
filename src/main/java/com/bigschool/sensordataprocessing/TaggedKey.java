package com.bigschool.sensordataprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hdhamee on 4/12/16.
 */
public class TaggedKey implements Writable, WritableComparable<TaggedKey> {
    private Text joinKey = new Text();
    private IntWritable tag = new IntWritable();

    @Override
    public int compareTo(TaggedKey taggedKey) {
        int compareValue = this.joinKey.compareTo(taggedKey.getJoinKey());
        if (compareValue == 0) {
            compareValue = this.tag.compareTo(taggedKey.getTag());
        }
        return compareValue;
    }

    public IntWritable getTag() {
        return tag;
    }

    public Text getJoinKey() {
        return joinKey;
    }

    public void set(String joinKey, int tag) {
        this.joinKey = new Text(joinKey);
        this.tag = new IntWritable(tag);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, joinKey.toString());
        tag.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey = new Text(Text.readString(in));
        tag = new IntWritable(in.readInt());
    }
}
