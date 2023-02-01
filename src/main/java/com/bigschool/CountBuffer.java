package com.bigschool;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

public class CountBuffer extends BaseOperation implements Function {
    int limit = 6;

    public CountBuffer(Fields newFields) {
        super(newFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry entry = functionCall.getArguments();
        functionCall.getOutputCollector().add(new Tuple(entry.getString("a").hashCode() % limit));
    }
}
