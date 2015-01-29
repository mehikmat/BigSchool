package com.bigschool;

import com.bigschool.mapper.DistributedCacheMapper;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class FileReadWriteTest extends TestCase {

    // test sequence file read and write
    @Test
    public void testSequenceFileRW() throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path("data/test.seq");
        fs.delete(file, true);

        SequenceFile.Writer.Option r1 = SequenceFile.Writer.file(file);
        SequenceFile.Writer.Option r2 = SequenceFile.Writer.keyClass(IntWritable.class);
        SequenceFile.Writer.Option r3 = SequenceFile.Writer.valueClass(Text.class);

        SequenceFile.Writer writer= SequenceFile.createWriter(conf,r1,r2,r3);
        writer.append(new IntWritable(1), new Text("one"));
        writer.append(new IntWritable(2), new Text("two"));
        writer.close();

        SequenceFile.Reader.Option option = SequenceFile.Reader.file(file);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf,option);

        IntWritable k = new IntWritable();
        Text v = new Text();

        reader.next(k,v);
        assertEquals(new IntWritable(1), k);
        assertEquals(new Text("one"),v);

        assertEquals(new Text("one"),reader.getCurrentValue((Object)null));
        assertEquals(new IntWritable(2),reader.next((Object)null));
        assertEquals(new Text("two"),reader.getCurrentValue((Object)null));
        assertNull(reader.next((Object)null));

        reader.close();
    }

    @Test
    public void testReadingTextFile() throws Exception{
        Path myFile = new Path("data/Student.csv");
        List<String> results = readTextFiles(myFile, new Configuration());

        String expected = "#student_first_name;student_middle_name;student_last_name;student_address;student_phone;student_roll;student_marks";
        assertEquals(expected,results.get(0));
    }

    @Test
    public void testReadingSequenceFile() throws Exception{
        // example usage
        Path myFile = new Path("data/test.seq");
        List<String> results = readSequenceFile(myFile, new Configuration(), IntWritable.class, Text.class);

        String expected = "[1;one, 2;two]";
        assertEquals(expected,results.toString());
    }

    /**
     * DistributedCache is a facility provided by the Map-Reduce framework to
     * cache files (text, archives, jars etc.) needed by applications.
     * Distribute cache, a application-specific large, read-only files.
     *
     * Applications specify the files, via urls (hdfs:// or http://) to be cached via the JobConf.
     * The DistributedCache assumes that the files specified via hdfs:// urls are already present on the FileSystem
     * at the path specified by the url.
     *
     * The framework will copy the necessary files on to the slave node before any tasks
     * for the job are executed on that node. Its efficiency stems from the fact that the
     * files are only copied once per job and the ability to cache archives which
     * are un-archived on the slaves.     *
     *
     * // Setting up the cache for the application
     *
     *   1. Copy the requisite files to the FileSystem:
     *      $ bin/hadoop fs -copyFromLocal lookup.dat /myapp/lookup.dat
     *
     *   2. Setup the application's Job:
     *      Job job = new Job();
     *      job.addCacheFile(new URI("data/Student.csv"));
     *
     *    3. Use the cached files in the Mapper or Reducer:
     *       configure files in configure method and use in reduce or map method.
     */
    // mapDriver runs in local mode (not in distributed/pseudo) single jvm
    // so no distributed cache available and returns null.
    @Test
    public void testDistributedCacheFiles() throws IOException{
        MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;

        DistributedCacheMapper mapper = new DistributedCacheMapper();
        mapDriver = MapDriver.newMapDriver(mapper);

        Configuration conf = new Configuration();
        conf.set(MRJobConfig.CACHE_LOCALFILES, "data/Student.csv");
        mapDriver.setConfiguration(conf);

        mapDriver.withInput(new LongWritable(1),new Text(""));

        List<Pair<Text,IntWritable>> list =  mapDriver.run();

        System.out.println("CACHE CONTENT:>> " + list.toString());

    }

    //In bash you can read any text-format file in hdfs (compressed or not), using the following command:
    //hadoop fs -text /path/to/your/file.gz
    // Reading text file(compressed or not)
    public List<String> readTextFiles(Path location, Configuration conf) throws Exception {
        FileSystem fileSystem = FileSystem.get(location.toUri(), conf);
        CompressionCodecFactory factory = new CompressionCodecFactory(conf);
        FileStatus[] items = fileSystem.listStatus(location);

        if (items == null) return new ArrayList<String>();
        List<String> results = new ArrayList<String>();

        for(FileStatus item: items) {
            // ignoring files like _SUCCESS
            if(item.getPath().getName().startsWith("_")) {
                continue;
            }
            CompressionCodec codec = factory.getCodec(item.getPath());
            InputStream stream = null;

            // check if we have a compression codec we need to use
            if (codec != null) {
                stream = codec.createInputStream(fileSystem.open(item.getPath()));
            }else {
                stream = fileSystem.open(item.getPath());
            }
            StringWriter writer = new StringWriter();
            IOUtils.copy(stream, writer, "UTF-8");
            String raw = writer.toString();
            Collections.addAll(results, raw.split("\n"));
        }
        return results;
    }

    public <A extends Writable, B extends Writable> List<String> readSequenceFile(Path path,
                                                                                  Configuration conf, Class<A> acls, Class<B> bcls) throws Exception {

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));

        A key = acls.newInstance();
        B value = bcls.newInstance();

        List<String> results = new ArrayList<String>();
        while(reader.next(key,value)) {
            results.add(key + ";" + value);
            key = acls.newInstance();
            value = bcls.newInstance();
        }
        return results;
    }
}
