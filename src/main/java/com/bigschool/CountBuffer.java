package com.bigschool;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import java.util.Random;

public class CountBuffer extends BaseOperation implements Function {
    Random random = new Random();
    int limit = 5;

    public CountBuffer(Fields newFields) {
        super(newFields);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        functionCall.getOutputCollector().add(new Tuple(random.nextInt(limit)));
    }
}
