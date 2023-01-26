package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tap.hadoop.PartitionTap;
import cascading.tap.partition.DelimitedPartition;
import cascading.tuple.Fields;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int i = 1; i <= 30000000; i++) {
            builder.append((i + "\n"));
            if (i % 1000000 == 0) {
                Files.write(Paths.get("data/input" + i + ".txt"), builder.toString().getBytes(), StandardOpenOption.APPEND);
                builder.setLength(0);
            }
        }

        //new Main().run();
    }

    public void run() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a"), ";"), "data/input.txt", SinkMode.KEEP);
        Hfs snkHfs = new Hfs(new TextDelimited(new Fields("a"), ";"), "output1", SinkMode.REPLACE);
        Tap snk1 = new PartitionTap(snkHfs, new DelimitedPartition(new Fields("b", "a")), SinkMode.REPLACE, false, 32768);

        Pipe pipe1 = new Pipe("copy1");
        pipe1 = new Each(pipe1, new CountBuffer(new Fields("b")), Fields.ALL);

        FlowConnector flowConnector = new HadoopFlowConnector();
        Flow flow1 = flowConnector.connect("Flow-1", src1, snk1, pipe1);
        flow1.complete();
    }
}
