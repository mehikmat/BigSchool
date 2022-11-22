package cascading.scheme.hadoop;

import cascading.flow.FlowProcess;
import cascading.scheme.format.CustomParquetOutputFormat;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.cascading.ParquetTupleScheme;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

public class CustomParquetTupleScheme extends ParquetTupleScheme {
    public CustomParquetTupleScheme() {
        super();
    }

    public CustomParquetTupleScheme(Fields sourceFields) {
        super(sourceFields);
    }

    public CustomParquetTupleScheme(FilterPredicate filterPredicate) {
        super(filterPredicate);
    }

    public CustomParquetTupleScheme(FilterPredicate filterPredicate, Fields sourceFields) {
        super(filterPredicate, sourceFields);
    }

    public CustomParquetTupleScheme(Fields sourceFields, Fields sinkFields, String schema) {
        super(sourceFields, sinkFields, schema);
    }

    public void sinkConfInit(FlowProcess<? extends JobConf> fp, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf jobConf) {
        super.sinkConfInit(fp,tap,jobConf);
        CustomParquetOutputFormat.setCompression(jobConf, CompressionCodecName.SNAPPY);
        jobConf.setOutputFormat(CustomParquetOutputFormat.class);
        jobConf.setBoolean("mapred.output.compress", false);
        jobConf.setBoolean("parquet.enable.summary-metadata", false);
    }
}
