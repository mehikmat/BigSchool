package com.bigschool;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import redis.clients.jedis.Jedis;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class FilterFunction extends BaseOperation implements Function {

    private Jedis jedis=null;

    public FilterFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
        if(jedis == null){
            String ip="localhost";
            jedis=new Jedis(ip);

            System.out.println("jedis connected to "+ip);
        }
    }

    @Override
    public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
        if(jedis!=null){
            jedis.close();
        }
    }


    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        String id = null;//entry.gestring()
        if(jedis.exists(id)){
            // return false;
        }
    }
}
