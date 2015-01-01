package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.aggregator.Count;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 *
 */
public class Main {

    public void run() {
        Tap src = new Hfs(new TextDelimited(new Fields("line")), "input/input.txt", SinkMode.KEEP);
        Tap snk = new Hfs(new TextDelimited(new Fields("line")), "output", SinkMode.REPLACE);

        Pipe pipe = new Pipe("copy");
        pipe=new GroupBy(pipe,new Fields("line"));
        pipe=new Every(pipe,new Count(new Fields("line")),Fields.RESULTS);

        Flow flow = new HadoopFlowConnector().connect(src, snk, pipe);

        flow.complete();

    }

    public static void main(String[] args) {
        new Main().run();
    }



}
