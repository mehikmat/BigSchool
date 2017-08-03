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
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) {
        if(args.length < 3){
            System.out.println("Insufficient arguments: jobName input output");
            System.exit(1);
        }
        Tap src1 = new Hfs(new TextDelimited(new Fields("a","b","c"),";"), args[1], SinkMode.KEEP);
        Tap snk1 = new Hfs(new TextDelimited(new Fields("a","b","c"),";"), args[2], SinkMode.REPLACE);

        Pipe pipe = new Pipe("test");

        Flow flow = new HadoopFlowConnector().connect(args[0],src1, snk1, pipe);
        flow.complete();
    }
}
