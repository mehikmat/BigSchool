package com.bigschool.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

import static com.bigschool.driver.apllications.DistributedCacheApplication.MASTER_PATH;
import static com.bigschool.driver.apllications.DistributedCacheApplication.OFFSET_PATH;

/**
 *
 */
public class LookupMapper extends Mapper<LongWritable, Text, Text, Text> {
    Map<String, Long> offsetIndexMap = new HashMap<>(3184105);
    RandomAccessFile raf;

    @Override
    protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        super.setup(context);
        // creates an object of Scanner
        Scanner input = new Scanner(Files.newInputStream(Paths.get(OFFSET_PATH)));

        while (input.hasNext()) {
            String line = input.nextLine();
            String[] part = line.split("\\|");

            offsetIndexMap.put(part[0], Long.parseLong(part[1]));
        }
        // closes the scanner
        input.close();

        raf = new RandomAccessFile(MASTER_PATH, "r");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] row = value.toString().split("\\|");

        Text k = new Text();
        Text v = new Text();

        if (offsetIndexMap.containsKey(row[0])) {
            raf.seek(offsetIndexMap.get(row[0]));
            k.set(raf.readLine());
            v.set(row[0]);
            context.write(k, v);
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
