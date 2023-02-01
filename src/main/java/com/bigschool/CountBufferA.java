package com.bigschool;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.Iterator;

public class CountBufferA extends BaseOperation implements Buffer {

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        String memberId = bufferCall.getGroup().getTuple().toString().replace("\"", "");
        int parent = memberId.hashCode() % 7;
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);

            OutputStream out1 = fs.create(new Path(buildSplitOutputPathWithRecordType(memberId, parent)), true);
            BufferedWriter br1 = new BufferedWriter(new OutputStreamWriter(out1));

            Iterator<TupleEntry> rows = bufferCall.getArgumentsIterator();
            while (rows.hasNext()) {
                TupleEntry argument = rows.next();
                br1.write("\"" + argument.getTuple().toString("\"|\"", false) + "\"");
                br1.newLine();
            }
            br1.flush();
            br1.close();

        } catch (IOException e) {
            throw new RuntimeException("Error while writing data of memberId: " + memberId + " StackTrace: " + e.getLocalizedMessage());
        }
    }

    private String buildSplitOutputPathWithRecordType(String memberId, int parent) {
        System.out.println("memberDataHDFSPathForCa-------> " + "memberDataHDFSPathForCa");
        return "memberDataHDFSPathForCa/" + parent +
                "/" + memberId + ".csv";
    }
}

