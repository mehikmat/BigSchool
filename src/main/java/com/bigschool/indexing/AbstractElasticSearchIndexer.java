package com.bigschool.indexing;

import com.bigschool.context.AppContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.UpdateSettingsRequest;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.HashMap;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public abstract class AbstractElasticSearchIndexer implements AlgorithmLifeCycle {
    Logger logger = Logger.getLogger(ElasticSearchIndexer.class);

    // constants
    public static final String INDEX_NAME = "index_name";
    public static final String elasticSearchClusterAddress = "es_node_addresses";
    public static final String elasticSearchClusterName = "cluster_name";
    public static final String PARAM_RUN_TRANSPORT = "transport_client";

    // cluster parameters
    protected String indexName;
    protected String recordType;
    protected String documentId;

    // ES clients
    protected Client client;
    protected Node node;
    protected ActionRequestBuilder request;

    // app config
    protected TaskInputOutputContext mapReduceContext = null;

    protected Integer requestCounter = 0;
    protected boolean runAsTransportClient =false;

    @Override
    public void setUp(AppContext context) {
        // check app config
        if(context.mapContext != null)
            mapReduceContext = context.mapContext;
        else if (context.reduceContext != null)
            mapReduceContext = context.reduceContext;

        // get configuration
        indexName = mapReduceContext.getConfiguration().get(INDEX_NAME);
        String esAddress = mapReduceContext.getConfiguration().get(elasticSearchClusterAddress);
        String esName = mapReduceContext.getConfiguration().get(elasticSearchClusterName);
        runAsTransportClient = mapReduceContext.getConfiguration().getBoolean(PARAM_RUN_TRANSPORT, runAsTransportClient);
        String[] arrESAddresses = esAddress.split(";");

        if(indexName ==null){
            throw new RuntimeException("index name not specified to run the index. Please change confing file to add 'index_name' property");
        }
        System.out.println("ES index being used:>> "+ indexName);

        if (runAsTransportClient) {
            System.out.println("Running as transport client...");
            Settings settings = ImmutableSettings.settingsBuilder()
                    .put("cluster.name", esName).build();
            client = new TransportClient(settings);
            for (String node : arrESAddresses) {
                System.out.println("Adding node:>> "+node);
                ((TransportClient) client).addTransportAddress(new InetSocketTransportAddress(node, 9300));
            }
        } else {
            if (esName != null) {
                System.out.println("Cluster Name:>> " + esName);
                System.out.println("cluster address::>> ");{
                    for (String add : arrESAddresses) {
                        System.out.print(add);
                    }
                }
                ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder();
                builder.put("cluster.name", esName);
                builder.put("discovery.zen.multicast.enabled", false);
                builder.putArray("discovery.zen.ping.unicast.hosts", arrESAddresses);
                builder.put("node.local", "false");
                builder.put("transport.tcp.connect_timeout", "1200s");

                //Settings settings = ImmutableSettings.settingsBuilder().put("cluster.name", esName).put("transport.tcp.connect_timeout","300s").build();
                node = NodeBuilder.nodeBuilder().settings(builder).client(true).clusterName(esName).node();
                client = node.client();
            } else {
                System.out.println("Cluster Name does not exists. Using default Transport ");
                node = NodeBuilder.nodeBuilder().local(true).node();
                client = node.client();
            }
        }

        logger.debug("This is client " + client.toString());

        /* Turns off the refreshing of the index */
        AdminClient admin = client.admin();
        IndicesAdminClient indicesAdminClient = admin.indices();
        UpdateSettingsRequest updateSettingsRequest = new UpdateSettingsRequest(indexName);
        HashMap<String, String> map = new HashMap<String, String>();
        map.put("index.refresh_interval", "-1");
        map.put("index.number_of_replicas", "0");
        map.put("index.policy.merge_factor", "30");
        updateSettingsRequest.settings(map);
        indicesAdminClient.updateSettings(updateSettingsRequest);
    }

    @Override
    public void cleanUp(AppContext context) {
        System.out.println("Cleanup is called at abstract method...");
        cleanUpInternal(context);
        if (client != null) {
            logger.info("Closing client...");
            client.close();
        }
        if (node != null) {
            logger.info("Closing node...");
            node.close();
        }
    }

    /**
     * Cleanup that will be called before cleaning the resources.
     * To be implemented by sub classes
     *
     * @param context
     */
    protected void cleanUpInternal(AppContext context) {
        //do nothing in default
    }

    protected void waitForDataTransfer(AbstractListener listener) {
        int i = 0;
        while (!listener.getStatus()) {
            try {
                if (i++ >= 100) {
                    i = 0;
                    System.out.println("Sending progress signal to prove this job is alive...");
                    mapReduceContext.progress();
                }
                System.out.println("Checking status:>> " + listener.getStatus());
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Exception while waiting>> " + e.getMessage());
            }
        }
        if(listener.hasError()){
            //Leave the underlying platform (Hadoop) to handle the task error.
            throw new RuntimeException(listener.getError());
        }
    }

    protected abstract AbstractListener getListener();

    protected void executeRequest(final boolean isCleanup) {
        System.out.println("Init listener...");
        logger.info("Starting thread for listener...");
        mapReduceContext.progress();
        mapReduceContext.getCounter("ESRecords","Written").increment(this.requestCounter);

        AbstractListener listener = getListener();
        Thread requestThread = new Thread(new RequestRunnable(request, listener));
        requestThread.setDaemon(false);
        requestThread.start();

        logger.info("Waiting for listener thread to finish writing...");
        waitForDataTransfer(listener);

        if (!isCleanup) {
            prepareRequest();
        }

    }

    protected void prepareRequest() {
        //prepare bulk request by default
        request = client.prepareBulk();
    }

    /**
     * Runnable class for request execution in separate thread
     */
    static class RequestRunnable implements Runnable {
        ActionRequestBuilder request;
        AbstractListener listener;

        public RequestRunnable(ActionRequestBuilder request, AbstractListener listener) {
            this.request = request;
            this.listener = listener;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void run() {
            request.execute(listener);
        }
    }

}
