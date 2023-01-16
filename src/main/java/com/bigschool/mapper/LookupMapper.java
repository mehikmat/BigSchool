package com.bigschool.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LookupMapper extends Mapper<LongWritable, Text, Text, Text> {
    Map<String, Integer> offsetIndexMap = new HashMap<>();
    RandomAccessFile raf;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        List<String> lines = Files.readAllLines(Paths.get("/usr/hadoop/offsetIndex.csv"));

        for(String line : lines){
            String[] part = line.split(",");
            offsetIndexMap.put(part[0], Integer.parseInt(part[1]));
        }

        raf = new RandomAccessFile("/usr/hadoop/masterTable.csv","r");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] row = value.toString().split(",");

        if (offsetIndexMap.containsKey(row[0])) {
            raf.seek(offsetIndexMap.get(row[0]));
            context.write(new Text(raf.readLine()), new Text(row[0]));
        }
    }

    @Override
    protected void cleanup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.cleanup(context);
        if(raf != null){
            raf.close();
        }
    }
}
