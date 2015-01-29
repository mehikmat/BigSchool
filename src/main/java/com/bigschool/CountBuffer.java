package com.bigschool;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class CountBuffer extends BaseOperation implements Buffer {

    public CountBuffer() {
        super(Fields.ARGS);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();
        while (iterator.hasNext()){
            TupleEntry entry = new TupleEntry(iterator.next());
            bufferCall.getOutputCollector().add(entry);
        }
    }
}
