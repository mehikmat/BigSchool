package com.bigschool;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.FlowProcess;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.CustomParquetTupleScheme;
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

    static public String getScheme(Fields outputFields) {
        StringBuilder schema = new StringBuilder();
        if (outputFields.size() > 0) {

            for (int i = 0; i < outputFields.size(); i++)
                schema.append("optional Binary " + outputFields.get(i) + ";");
        }
        return schema.toString();
    }

    public void run() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a", "b", "c"), ","), "input", SinkMode.KEEP);
        Tap snk1 = new Hfs(new CustomParquetTupleScheme(new Fields("a", "b", "c"), new Fields("a", "b", "c"), "message TestSchema {" + getScheme(new Fields("a", "b", "c")) + "}"), "output", SinkMode.REPLACE);

        Pipe pipe1 = new Pipe("copy");
        pipe1 = new GroupBy(pipe1, new Fields("a"));
        pipe1 = new Every(pipe1, new CopyBuffer(), Fields.RESULTS);
        pipe1 = new Each(pipe1, new MyFilter());

        Flow flow = new HadoopFlowConnector().connect("Flow-1", src1, snk1, pipe1);

        pipe1.getConfigDef().setProperty("mapred.reduce.tasks", "2");

        CascadeConnector connector = new CascadeConnector();
        Cascade cascade = connector.connect(flow);
        cascade.complete();

    }

    static public class MyFilter extends BaseOperation implements Filter {

        public MyFilter() {
        }

        @Override
        public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
            return false;
        }
    }

    public static void main(String[] args) {
        new Main().run();
    }
}
