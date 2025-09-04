package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.hadoop2.Hadoop2MR1FlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

public class Main {

    public static void main(String[] args) {
        String inputFile = "input.csv";
        String outputFile = "output.csv";
        Hfs src1 = new Hfs(new TextDelimited(new Fields("a", "b", "c"), ";"), inputFile, SinkMode.KEEP);
        Hfs snk1 = new Hfs(new TextDelimited(new Fields("a", "b", "c"), ";"), outputFile, SinkMode.REPLACE);

        Pipe pipe = new Pipe("test");

        Hadoop2MR1FlowConnector hadoop2MR1FlowConnector = new Hadoop2MR1FlowConnector();
        Flow hadoopFlow = hadoop2MR1FlowConnector.connect(inputFile, src1, snk1, pipe);
        hadoopFlow.complete();
    }
}
