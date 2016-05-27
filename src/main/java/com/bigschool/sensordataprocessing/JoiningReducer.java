package com.bigschool.sensordataprocessing;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hdhamee on 4/12/16.
 */
public class JoiningReducer extends Reducer<TaggedKey, Text, NullWritable, Text> {

    private Text joinedText = new Text();
    private NullWritable nullKey = NullWritable.get();

    @Override
    protected void reduce(TaggedKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        boolean first = true;
        StringBuilder leftSideRecord = new StringBuilder();

        for (Text value : values) {
            StringBuilder builder = new StringBuilder();

            if (first){
                leftSideRecord.append(key.getJoinKey()).append(",").append(value.toString());
                first = false;
            }else {
                builder.append(leftSideRecord).append(",");
                builder.append(value.toString());

                builder.setLength(builder.length() - 1);
                joinedText.set(builder.toString());
                context.write(nullKey, joinedText);
                builder.setLength(0);
            }
        }
    }
}
