package com.bigschool.indexing;

import com.bigschool.context.AppContext;
import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class ElasticSearchIndexer extends AbstractElasticSearchIndexer implements IndexingAlgorithm {

    //Size we let the bulk request get to before commit;
    private static final int requestSize = 79999;
    protected int requestCounter = 0;
    protected int processCounter = 0;
    protected Map<byte[], String> stringMap;
    protected static String colon = ":";
    protected XContentBuilder jsonBuilder;
    Logger logger = Logger.getLogger(ElasticSearchIndexer.class);

    @Override
    public void setUp(AppContext context) {
        super.setUp(context);
        //First time in
        if (request == null) {
            request = client.prepareBulk();
        }
    }

    @Override
    public void startRecord(String documentId) {
        this.documentId = documentId;

        // Record level initialization
        try {
            jsonBuilder = jsonBuilder();
            jsonBuilder.startObject();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // Increment the bulk record counter
        requestCounter++;
    }

    public void startArray(String name) {
        try {
            jsonBuilder.startArray(name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void endArray() {
        try {
            jsonBuilder.endArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startObject() {
        try {
            jsonBuilder.startObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void startObject(String name) {
        try {
            jsonBuilder.startObject(name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void endObject() {
        try {
            jsonBuilder.endObject();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void processColumn(String columnName, Object data) {
        try {
            jsonBuilder.field(columnName, data);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void endRecord() {
        try {
            jsonBuilder.endObject();
            System.out.println("Document "+ requestCounter + " added to bulk request.");

            ((BulkRequestBuilder) request).add(Requests.indexRequest(indexName).type(recordType).id(documentId).create(false).source(jsonBuilder));

            if (requestCounter > requestSize) {
                System.out.println("Submitting " + requestCounter + " documents.");
                processCounter = processCounter + requestCounter;
                executeRequest(false);
                requestCounter = 0;
            }
        } catch (IOException e) {
            // rethrow the exception, just bubble up
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     * A separate thread required to make execute call asynchronous so that regular
     * call to context that this map task is not hanged but is processing can be done.
     */
    private static class ListenerThread extends Thread {
        private final ResponseListener listener;
        private final BulkRequestBuilder bulkRequest;

        public ListenerThread(ResponseListener listener, BulkRequestBuilder bulkRequest) {
            this.listener = listener;
            this.bulkRequest = bulkRequest;
        }

        @Override
        public void start() {
            super.start();
            bulkRequest.execute(listener);
        }
    }

    @Override
    public void cleanUpInternal(AppContext context) {
        /* Execute the last batch */
        System.out.println("Cleaning up the bulk request... " + requestCounter);
        if (requestCounter > 0) {
            System.out.println("Clean up is submitting " + requestCounter + " document.");
            processCounter = processCounter + requestCounter;
            if (mapReduceContext == null) {
                if (context.mapContext != null)
                    mapReduceContext = context.mapContext;
                else
                    mapReduceContext = context.mapContext;
            }
            System.out.println("Calling executeRequest from cleanup...");
            executeRequest(true);
        }
        System.out.println("Finished processing " + processCounter + " documents.");
    }

    protected AbstractListener getListener() {
        return new ResponseListener();
    }

    public String toString(byte[] bytes) {
        String strReturn = stringMap.get(bytes);
        if (strReturn == null) {
            strReturn = new String(bytes).trim();
            stringMap.put(bytes, strReturn);
        }
        return (strReturn);
    }
}
