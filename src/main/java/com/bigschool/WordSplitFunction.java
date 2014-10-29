package com.bigschool;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class WordSplitFunction extends BaseOperation implements Function {
    public WordSplitFunction(){
       super(new Fields("ip"));
    }
    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        String value = functionCall.getArguments().getTupleCopy().toString();
        for (String v : value.split(";")){
            functionCall.getOutputCollector().add(new Tuple(v));
        }
    }
}
