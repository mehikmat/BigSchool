package com.bigschool.yarn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by hdhamee on 5/30/14.
 */
public class ApplicationMaster{

    private static final Logger LOG = Logger.getLogger(ApplicationMaster.class.getName());
    private AMRMClientAsync<AMRMClient.ContainerRequest> resourceManager;

    private NMClient nodeManager;
    private Configuration conf;
    private String commandTemplate="";

    private int containerMem=300;
    protected int totalContainerCount=4;
    private String command="echo";

    private AtomicInteger completedContainerCount;
    private AtomicInteger allocatedContainerCount;
    private AtomicInteger failedContainerCount;
    private AtomicInteger requestedContainerCount;

    private String appMasterHostname = "localhost"; // TODO: What should this really be?
    private int appMasterRpcPort = 9999; // TODO: What should this really be?
    private String appMasterTrackingUrl = "localhost"; // TODO: What should this really be?

    private boolean done;
    protected Map<ContainerId, String> containerIdCommandMap;
    protected List<String> failedCommandList;

    public ApplicationMaster() {
        conf = new YarnConfiguration();
        completedContainerCount = new AtomicInteger();
        allocatedContainerCount = new AtomicInteger();
        failedContainerCount = new AtomicInteger();
        requestedContainerCount = new AtomicInteger();

        containerIdCommandMap = new HashMap<ContainerId, String>();
        failedCommandList = new ArrayList<String>();
    }

    public void init(String[] args) {
        LOG.setLevel(Level.INFO);
        done = false;
    }

    public boolean run() throws IOException, YarnException {
        // Initialize clients to RM and NMs.
        LOG.info("ApplicationMaster::run");
        LOG.error("command: " + this.command);
        AMRMClientAsync.CallbackHandler rmListener = new RMCallbackHandler();
        resourceManager = AMRMClientAsync.createAMRMClientAsync(1000, rmListener);
        resourceManager.init(conf);
        resourceManager.start();

        nodeManager = NMClient.createNMClient();
        nodeManager.init(conf);
        nodeManager.start();

        // Register with RM
        resourceManager.registerApplicationMaster(appMasterHostname, appMasterRpcPort, appMasterTrackingUrl);


        // Ask RM to give us a bunch of containers
        for (int i = 0; i < totalContainerCount; i++) {
            AMRMClient.ContainerRequest containerReq = setupContainerReqForRM();
            resourceManager.addContainerRequest(containerReq);
        }
        requestedContainerCount.addAndGet(totalContainerCount);

        while (!done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException ex) {
            }
        }// while

        // Un-register with ResourceManager
        resourceManager.unregisterApplicationMaster( FinalApplicationStatus.SUCCEEDED, "", "");
        return true;
    }

    private AMRMClient.ContainerRequest setupContainerReqForRM() {
        // Priority for worker containers - priorities are intra-application
        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);
        // Resource requirements for worker containers
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(containerMem);
        //capability.setVirtualCores(1);
        AMRMClient.ContainerRequest containerReq = new AMRMClient.ContainerRequest(
                capability,
                null /* hosts String[] */,
                null /* racks String [] */,
                priority);
        return containerReq;
    }

    private synchronized void recordFailedCommand(ContainerId cid) {
        String failedCmd = containerIdCommandMap.get(cid);
        containerIdCommandMap.remove(cid);
        failedCommandList.add(failedCmd);
    }

    private List<String> buildCommandList(int startingFrom, int containerCnt, String command){
        // TODO Auto-generated method stub
        List<String> r = new ArrayList<String>();
        int stopAt = startingFrom + containerCnt;
        for (int i = startingFrom; i < stopAt; i++) {
            StringBuilder sb = new StringBuilder();
            sb.append(commandTemplate).append(" ").append(String.valueOf(i));
            String cmd = sb.toString();
            LOG.info("curr i : " + i);
            LOG.info(cmd);
            r.add(cmd);
        }
        return r;

    }

    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
        // CallbackHandler for RM.
        // Execute a program when the container is allocated
        // Reallocate upon failure.


        public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus status: statuses) {
                assert (status.getState() == ContainerState.COMPLETE);

                int exitStatus = status.getExitStatus();
                if (exitStatus != ContainerExitStatus.SUCCESS) {
                    if (exitStatus != ContainerExitStatus.ABORTED) {
                        failedContainerCount.incrementAndGet();
                    }
                    allocatedContainerCount.decrementAndGet();
                    requestedContainerCount.decrementAndGet();
                    recordFailedCommand(status.getContainerId());
                } else {
                    completedContainerCount.incrementAndGet();
                }
            }

            int askAgainCount = totalContainerCount - requestedContainerCount.get();
            requestedContainerCount.addAndGet(askAgainCount);

            if (askAgainCount > 0) {
                // need to reallocate failed containers
                for (int i = 0; i < askAgainCount; i++) {
                    AMRMClient.ContainerRequest req = setupContainerReqForRM();
                    resourceManager.addContainerRequest(req);
                }
            }

            if (completedContainerCount.get() == totalContainerCount) {
                done = true;
            }
        }

        public void onContainersAllocated(List<Container> containers) {
            int containerCnt = containers.size();
            List<String> cmdLst;

            if (failedCommandList.isEmpty()) {
                int startFrom = allocatedContainerCount.getAndAdd(containerCnt);
                LOG.error("containerCnt: " + containerCnt);
                cmdLst = buildCommandList(startFrom, containerCnt, command);
            } else {
                // TODO: keep track of failed commands' history.
                cmdLst = failedCommandList;
                int failedCommandListCnt = failedCommandList.size();
                if (failedCommandListCnt < containerCnt) {
                    // It's possible that the allocated containers are for both newly allocated and failed containers
                    int startFrom = allocatedContainerCount.getAndAdd(containerCnt - failedCommandListCnt);
                    cmdLst.addAll(buildCommandList(startFrom, containerCnt, command));
                }
            }

            for (int i = 0; i < containerCnt; i++) {
                Container c = containers.get(i);
                String cmdStr = cmdLst.remove(0);
                LOG.error("running cmd: " + cmdStr);
                StringBuilder sb = new StringBuilder();
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                containerIdCommandMap.put(c.getId(), cmdStr);
                ctx.setCommands(Collections.singletonList(
                        sb.append(cmdStr)
                                .append(" 1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout")
                                .append(" 2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr")
                                .toString()
                ));
                try {
                    nodeManager.startContainer(c, ctx);
                } catch (YarnException e) {
                    // TODO: what should I do here? reallocated a new container?
                } catch (IOException e) {
                    // TODO: what should I do here? reallocated a new container?
                }
            }
        }


        public void onNodesUpdated(List<NodeReport> updated) { }

        public void onError(Throwable e) {
            done = true;
            resourceManager.stop();
        }

        // Called when the ResourceManager wants the ApplicationMaster to shutdown
        // for being out of sync etc. The ApplicationMaster should not unregister
        // with the RM unless the ApplicationMaster wants to be the last attempt.
        public void onShutdownRequest() {
            done = true;
        }

        public float getProgress() {
            return 0;
        }
    }
    public static void main(String[] args) {
        System.out.println("ApplicationMaster::main"); //xxx
        ApplicationMaster am = new ApplicationMaster();
        am.init(args);
        try {
            am.run();
        } catch (Exception e) {
            System.out.println("am.run throws: " + e);
            e.printStackTrace();
            System.exit(0);
        }
    }
}