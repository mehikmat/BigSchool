package com.bigschool;

import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;

/**
 * Created by hdhamee on 6/2/14.
 */
public class Main {

    public static void main(String[] args) {
        new Main().run();
    }
   public void run() {
       Tap src = new Hfs(new TextDelimited(new Fields("line")), "input/input.txt", SinkMode.KEEP);
       Tap snk = new Hfs(new TextDelimited(new Fields("line")), "output", SinkMode.REPLACE);

       Pipe pipe = new Pipe("copy");

       Flow flow = new HadoopFlowConnector().connect(src, snk, pipe);

       flow.complete();

   }


}
