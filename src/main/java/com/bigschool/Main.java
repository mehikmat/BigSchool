package com.bigschool;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public void run() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a","b","c"),";"), "input/input.txt", SinkMode.KEEP);
        Tap snk1 = new Hfs(new TextDelimited(new Fields("a","b","c"),";"), "output1", SinkMode.REPLACE);

        Tap src2 = new Hfs(new TextDelimited(new Fields("a","b","c"),";"), "output1", SinkMode.REPLACE);

        Tap snk2 = new Hfs(new TextDelimited(new Fields("a","b","c"),";"), "output2", SinkMode.REPLACE);

        Pipe pipe1 = new Pipe("copy1");
        pipe1 = new GroupBy(pipe1,new Fields("a"));
        pipe1 = new Every(pipe1,new CountBuffer(),Fields.RESULTS);

        Pipe pipe2 = new Pipe("copy2");
        pipe2 = new GroupBy(pipe2,new Fields("a"));
        pipe2 = new Every(pipe2,new CountBuffer(),Fields.RESULTS);

        Flow flow1 = new HadoopFlowConnector().connect(src1, snk1, pipe1);
        Flow flow2 = new HadoopFlowConnector().connect(src2, snk2, pipe2);


        CascadeConnector connector = new CascadeConnector();
        Cascade cascade = connector.connect( flow1, flow2);
        cascade.complete();

    }

    public static void main(String[] args) {
        new Main().run();
    }
}
