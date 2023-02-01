package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.pipe.*;
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

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.Random;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) throws IOException {
        if (args[0].equals("generate")) {
            StringBuilder builder = new StringBuilder();
            for (int i = 1; i <= 30000000; i++) {
                builder.append((i + "\n"));
                if (i % 10000 == 0) {
                    if (!new File("Medical/part-" + i + ".csv").exists()) {
                        new File("Medical/part-" + i + ".csv").createNewFile();
                    }
                    if (!new File("Eligibility/part-" + i + ".csv").exists()) {
                        new File("Eligibility/part-" + i + ".csv").createNewFile();
                    }
                    if (!new File("Pharmacy/part-" + i + ".csv").exists()) {
                        new File("Pharmacy/part-" + i + ".csv").createNewFile();
                    }
                    Files.write(Paths.get("Medical/part-" + i + ".csv"), builder.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get("Eligibility/part-" + i + ".csv"), builder.toString().getBytes(), StandardOpenOption.APPEND);
                    Files.write(Paths.get("Pharmacy/part-" + i + ".csv"), builder.toString().getBytes(), StandardOpenOption.APPEND);
                    builder.setLength(0);
                }
            }
        } else {
            new Main().run();
        }
    }

    public void run() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a"), ";"), "Eligibility", SinkMode.KEEP);
        Tap src2 = new Hfs(new TextDelimited(new Fields("a"), ";"), "Medical", SinkMode.KEEP);
        Tap src3 = new Hfs(new TextDelimited(new Fields("a"), ";"), "Pharmacy", SinkMode.KEEP);
        Hfs snkHfs = new Hfs(new TextDelimited(new Fields("a"), ";"), "output1", SinkMode.REPLACE);
        Tap snk1 = new PartitionTap(snkHfs, new DelimitedPartition(new Fields("b", "a")), SinkMode.REPLACE, false, 600);

        Pipe pipe1 = new Pipe("src1");
        Pipe pipe2 = new Pipe("src2");
        Pipe pipe3 = new Pipe("src3");
        pipe1 = new Merge(pipe1, pipe2, pipe3);
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
