package com.bigschool.driver;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hdhamee on 4/12/16.
 */
public class JoiningReducer extends Reducer <TaggedKey, Text, NullWritable, Text> {

    private Text joinedText = new Text();
    private StringBuilder builder = new StringBuilder();
    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        builder.append(key.getJoinKey()).append(",");
        for (Text value : values) {
            builder.append(value.toString()).append(",");
        }
        builder.setLength(builder.length()-1);
        joinedText.set(builder.toString());
        context.write(nullKey, joinedText);
        builder.setLength(0);
    }
}
