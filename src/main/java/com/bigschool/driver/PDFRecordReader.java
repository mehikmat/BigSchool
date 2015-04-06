package com.bigschool.driver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;

import java.io.IOException;

/**
 * Record Reader:
 * A RecordReader uses the data within the boundaries created by the input split to generate
 * key/value pairs. In the context of file-based input, the “start” is the byte position
 * in the file where the RecordReader should start generating key/value pairs. The “end”
 * is where it should stop reading records. These are not hard boundaries as far as the
 * API is concerned—there is nothing stopping a developer from reading the entire file
 * for each map task. While reading the entire file is not advised, reading outside of
 * the boundaries it often necessary to ensure that a complete record is generated
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class PDFRecordReader extends RecordReader<LongWritable, Text> {
    private String[] lines = null;
    private LongWritable key = null;
    private Text value = null;

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        final Path file = split.getPath();

		/*
         * The below code contains the logic for opening the file and seek to
		 * the start of the split. Here we are applying the Pdf Parsing logic
		 */

        FileSystem fs = file.getFileSystem(job);
        FSDataInputStream fileIn = fs.open(split.getPath());
        PDDocument pdf = null;
        String parsedText = null;
        PDFTextStripper stripper;
        pdf = PDDocument.load(fileIn);
        stripper = new PDFTextStripper();
        parsedText = stripper.getText(pdf);
        this.lines = parsedText.split("\n");
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        // In the each mapper phase, initially key is null
        if (key == null) {
            key = new LongWritable();
            key.set(1);
            value = new Text();
            value.set(lines[0]);
            // now key is not null
        } else {
            int temp = (int) key.get();
            if (temp < (lines.length - 1)) {
                int count = (int) key.get();
                value = new Text();
                value.set(lines[count]);
                count = count + 1;
                key = new LongWritable(count);
            } else {
                return false;
            }

        }
        // indicates end of file
        return !(key == null || value == null);
    }

    @Override
    public LongWritable getCurrentKey() throws IOException,
            InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {

        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}

