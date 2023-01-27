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

import java.io.IOException;

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
        Tap snk1 = new PartitionTap(snkHfs, new DelimitedPartition(new Fields("b", "a")), SinkMode.REPLACE, false, 2500);

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
        pipe1 = new Each(pipe1, new CountBuffer(new Fields("b")), Fields.ALL);
        pipe1 = new GroupBy(pipe1, new Fields("a"));
        pipe1 = new Every(pipe1, new CountBufferA());

        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow1 = flowConnector.connect("Flow-1", src1, snkHfs, pipe1);
        flow1.complete();
    }

    public class CountBufferA extends BaseOperation implements Buffer {

        @Override
        public void operate(FlowProcess flowProcess, BufferCall bufferCall) {

        }
    }
}
