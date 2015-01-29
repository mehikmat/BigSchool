package com.bigschool.mapper;

import com.bigschool.context.AppContext;
import com.bigschool.indexing.ElasticSearchIndexer;
import com.bigschool.indexing.IndexingAlgorithm;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class StudentIndexMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IndexingAlgorithm indexingAlgorithm = new ElasticSearchIndexer();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        AppContext appContext = new AppContext();
        appContext.mapContext = context;
        indexingAlgorithm.setUp(appContext);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        indexingAlgorithm.startRecord(null, null, null);
        indexingAlgorithm.processColumn(null, null, null);
        indexingAlgorithm.endRecord();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        indexingAlgorithm.cleanUp(getAppContext(context));
    }

    private AppContext getAppContext(Mapper.Context context) {
        AppContext appContext = new AppContext();
        appContext.mapContext = context;
        return appContext;
    }
}
