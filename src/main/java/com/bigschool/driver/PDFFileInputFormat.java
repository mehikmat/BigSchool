package com.bigschool.driver;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * Input Split: As InputSplit is nothing more than a chunk of 1 or several blocks
 * <p/>
 * Input Format:
 * Hadoop relies on the input format of the job to do three things:
 * 1. Validate the input configuration for the job (i.e., checking that the data is there).
 * 2. Split the input blocks and files into logical chunks of type InputSplit, each of which is
 * assigned to a map task for processing.
 * 3. Create the RecordReader implementation to be used to create key/value pairs from the raw
 * InputSplit. These pairs are sent one by one to their mapper.
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class PDFFileInputFormat extends FileInputFormat<LongWritable, Text> {

    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        return new PDFRecordReader();
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
