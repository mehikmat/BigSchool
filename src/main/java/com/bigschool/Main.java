package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) throws IOException {
        /*StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= 30000000; i++) {
            builder.append((i + "\n"));
            if (i % 10000 == 0) {
                if (!new File("data/input" + i + ".txt").exists()) {
                    new File("data/input" + i + ".txt").createNewFile();
                }
                Files.write(Paths.get("data/input" + i + ".txt"), builder.toString().getBytes(), StandardOpenOption.APPEND);
                builder.setLength(0);
            }
        }*/

        new Main().run();
    }

    public void run() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a"), ";"), "data", SinkMode.KEEP);
        Hfs snkHfs = new Hfs(new TextDelimited(new Fields("a"), ";"), "output1", SinkMode.REPLACE);
        Tap snk1 = new PartitionTap(snkHfs, new DelimitedPartition(new Fields("b", "a")), SinkMode.REPLACE, false, 600);

        Pipe pipe1 = new Pipe("copy1");
        pipe1 = new Each(pipe1, new CountBuffer(new Fields("b")), Fields.ALL);

        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow1 = flowConnector.connect("Flow-1", src1, snk1, pipe1);
        flow1.complete();
    }

    public void run1() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a"), ";"), "data", SinkMode.KEEP);
        Hfs snkHfs = new Hfs(new TextDelimited(new Fields("a"), ";"), "output1", SinkMode.REPLACE);

        Pipe pipe1 = new Pipe("copy1");
        pipe1 = new GroupBy(pipe1, new Fields("a"));
        pipe1 = new Every(pipe1, new CountBufferA());

        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow1 = flowConnector.connect("Flow-1", src1, snkHfs, pipe1);
        flow1.complete();
    }

    public class CountBufferA extends BaseOperation implements Buffer {
        Random random = new Random();

        @Override
        public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
            String memberId = bufferCall.getGroup().getTuple().toString().replace("\"", "");
            try {
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(conf);

                OutputStream out1 = fs.create(new Path(buildSplitOutputPathWithRecordType(memberId, "Medical")), true);
                BufferedWriter br1 = new BufferedWriter(new OutputStreamWriter(out1));

                Iterator<TupleEntry> rows = bufferCall.getArgumentsIterator();
                while (rows.hasNext()) {
                    TupleEntry argument = rows.next();
                    br1.write("\"" + argument.getTuple().toString("\"|\"", false) + "\"");
                    br1.newLine();
                }
                br1.flush();
                br1.close();

            } catch (IOException e) {
                throw new RuntimeException("Error while writing data of memberId: " + memberId + " StackTrace: " + e.getLocalizedMessage());
            }
        }

        private String buildSplitOutputPathWithRecordType(String memberId, String recordType) {
            System.out.println("memberDataHDFSPathForCa-------> " + "memberDataHDFSPathForCa");
            return "memberDataHDFSPathForCa" + "/" + recordType + "/" + random.nextInt(10) +
                    "/" + memberId +
                    "/" + memberId + "_" + recordType + ".csv";
        }
    }
}
