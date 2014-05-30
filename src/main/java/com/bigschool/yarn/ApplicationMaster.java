package com.bigschool.yarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by hdhamee on 5/30/14.
 */
public class ApplicationMaster{
private static final Log LOG = LogFactory.getLog(ApplicationMaster.class);

//    ***** CLASS FIELD DECLARATIONS *****

// instance for YARN cluster configuration info.
// Used to initialize ApplicationMaster instances
private Configuration conf;

// Protocol handles that you will be using
// **Note the direction of communication for each handle.
//   AMRMClientAsync and NMClientAsync are handles
//     that are used for AM-induced operations,
//     such as AM registration (with RM) and
//     container launch and start (with NMs). The class
//     implements come with the YARN api.
//   RMCallbackHandler and NMCallbackHandler are handles
//     implemented within the AM with the CallbackHandler
//     interface of AMRMClientAsync and NMClientAsync. They
//     are used to deal with responses from RM and NMs,
//     such as receiving allocated container info's (from RM) and
//     container operation reports (from NM).
// **Also note that 'NMs' refer not only to the host NM of
//   the AM container, but also to all NMs that host
//   containers doing the actual execution.
//   NMClientAsync and NMCallbackHandler are handles for all
//   of these NMs.
private AMRMClientAsync resourceManager;     // AM --> RM
private RMCallbackHandler allocListener;     // AM  NMs
private NMCallbackHandler containerListener; // AM

// You will need these three to be passed as arguments when registering the AM with RM.
// However, the actual implementaion of these info is still a
// in the current Hadoop YARN release
private String appMasterHostname = "";    // AM container hostname
private int appMasterRpcPort = 0;         // Listen port for clients' status updates
private String appMasterTrackingUrl = ""; // url for clients to track AM info
private  NMClientAsync nmClientAsync;

// Simple example of info needed to request containers from RM
// (Actual AMs will be more complex)
private int numTotalContainers; // fixed no. of containers.
// actual AMs is likely to need flexible requests.
private int containerMemory;    // fixed memory size for each request.
// actual AMs will need different resources for different containers.
private int requestPriority;    // fixed request priority.
// actual AMs' containers should have different priority

// AtomicIntegers used to keep track of container counts
// and facilitate monitoring of container statuses
private AtomicInteger numCompletedContainers = new AtomicInteger(); // denotes successful + failed count
private AtomicInteger numAllocatedContainers = new AtomicInteger(); // denotes no. of containers RM has already allocated to AM
private AtomicInteger numFailedContainers = new AtomicInteger();    // denotes failed count
private AtomicInteger numRequestedContainers = new AtomicInteger(); // denotes no. of containers request from RM

// Simple example of info needed to set up ContainerLaunchContexts for execution containers
// (Actual AMs will be more complex)
private Map<String, String> containerEnv = new HashMap<String, String>();
// fixed env. setups for each container.
// actual AMs will have different env. setups for containers.

// flag used to control execution flow
private volatile boolean done;

// Thread list for execution container launch threads.
// (Containers are launched in independent threads)
private List<Thread> launchThreads = new ArrayList();

//    ***** CLASS METHOD DECLARATIONS *****

public static void main(String[] args) throws ParseException, IOException, YarnException {
        // create ApplicationMaster instance,
        // initialize and start running it
    ApplicationMaster appMaster = new ApplicationMaster();
    LOG.info("Initializing ApplicationMaster");
    appMaster.init(args);
    appMaster.run();
        }

public ApplicationMaster() {
        // constructor
        // initializes conf with YARN cluster configuration info
    conf = new YarnConfiguration();
        }

public void init(String[] args) throws ParseException {
        // set up command line argument options (explicitly used if AM ran under unmanaged mode, i.e. no client),
        // and parse argument values to initialize class fields.
    // set up command argument options
    Options opts = new Options();
    opts.addOption("container_env", true,
            "Environment for execution containers. Specified as env_key=env_val pairs");
    opts.addOption("container_memory", true,
            "Amount of memory in MB to be requested to run the execution containers");
    opts.addOption("num_containers", true,
            "No. of containers that needs to be executed");
    opts.addOption("priority", true, "Application Priority. Default 0");

    // command line parser instance
    CommandLine cliParser = new GnuParser().parse(opts, args);

    // parse "container_env" option for container environment values
    String containerEnvs[] = cliParser.getOptionValues("container_env");
    for (String env : containerEnvs) { // for all environment values stated
        //trimEnvtoKeyVal();               // helper function to trim env String to a key-value pair
        String[] keyVal=env.split(" ");
        containerEnv.put(keyVal[0], keyVal[1]);      // add env K-V pair to containerEnv
    }

    // parse argument option values to initialize corresponding class fields.
    // later used for container request setup
    containerMemory = Integer.parseInt(cliParser.getOptionValue(
            "container_memory", "10"));
    numTotalContainers = Integer.parseInt(cliParser.getOptionValue(
            "num_containers", "1"));
    requestPriority = Integer.parseInt(cliParser
            .getOptionValue("priority", "0"));
        }

public void run() throws IOException, YarnException {
        // main run function for AM.
        // initialize handlers, register AM with RM,
        // and request RM for containers (implemented in setupContainerAskForRM() method).
        // CallbackHandlers will automatically deal with RM allocation responses.
        // monitor 'done' flag to check for app progress completion. When all done,
        // release all containers and unregister AM from RM (implemented in finsh() method).
    LOG.info("Starting ApplicationMaster");

    // initialize AMRMClientAsync instance (resourceManager) with its corresponding CallbackHandler (allocListener).
    allocListener = new RMCallbackHandler();
    resourceManager =
            AMRMClientAsync.createAMRMClientAsync(1000, allocListener); // 1st arg: heartbeat interval,
    resourceManager.init(conf);  // basic YARN cluster configuration info initialization
    resourceManager.start();     // start AMRMClient handler + CallbackHandler

    // initialize NMClientAsync instance (nmClientAsync) with its corresponding CallbackHandler (containerListener)
    containerListener = new NMCallbackHandler();
    nmClientAsync = new NMClientAsyncImpl(containerListener);
    nmClientAsync.init(conf);    // basic YARN cluster configuration info intialization
    nmClientAsync.start();       // start NMClient handler + CallbackHandler

    // AM register with RM. Heartbeating starts after registration
    RegisterApplicationMasterResponse response = resourceManager
            .registerApplicationMaster(appMasterHostname, appMasterRpcPort,
                    appMasterTrackingUrl);        // note: the three passed arguments in the current beta release
    // have not actually been implemented yet

    // Setup container requests to be sent to RM
    // Simple example scenario used here is that the no. of containers needed is fixed,
    // each container requires the same computing resource (memory) and uses same priority.
    // actual AMs should be much  more complex
    for (int i = 0; i < numTotalContainers; ++i) {
        AMRMClient.ContainerRequest containerAsk = setupContainerAskForRM();
        // setupContainerAskForRM() sets up the request with resource amount and priority info
        resourceManager.addContainerRequest(containerAsk); // request from RM
    }
    numRequestedContainers.set(numTotalContainers); // now, numRequestedContainers == numTotalContainers

    // when all containers are completed, the RMCallbackHandler will automatically be invoked
    // and set done to 'true', thus leaving the sleep loop
    while (!done) {
        try {
            Thread.sleep(200);
        } catch (InterruptedException ex) {}
    }

    // release all containers and unregister AM from RM upon completion
    finish();
        }

private void finish() throws IOException, YarnException {
        // Join all execution container launch threads to stop all containers.
        // Finally, unregister and stop AM.
    // Join all launched threads
    // needed for when we time out
    // and we need to release containers
    for (Thread launchThread : launchThreads) {
        try {
            launchThread.join(10000);
        } catch (InterruptedException ignored) {
        }
    }

    // Determine the completion status of app by checking container AtomicInteger values.
    // Simple scenario used here is that failed containers will not be respawned,
    // thus any failed containers means application failure
    FinalApplicationStatus appStatus;
    if (numFailedContainers.get() == 0 &&
            numCompletedContainers.get() == numTotalContainers) {
        appStatus = FinalApplicationStatus.SUCCEEDED;
    } else {
        appStatus = FinalApplicationStatus.FAILED;
    }

    // Unregister AM with RM.
    // 1st arg: completion status, 2nd arg: display msg, 3rd arg: the tracking url (not yet implemented)
    resourceManager.unregisterApplicationMaster(appStatus, null, null);

    // set done to 'true' to terminate sleep loop in run()
    done = true;

    // terminate and release AMRMClientAsync and NMClientAsync handlers
    resourceManager.stop();
    nmClientAsync.stop();
        }

private AMRMClient.ContainerRequest setupContainerAskForRM() {
        // prepare container ask instance (ContainerRequest class)
        // with priority and resource requirement info
    // Simple scenario that each container request needs
    // the same amount of resource and uses same priority.
    // actual AMs are much more complex (specific requirements for diff. containers)

    // priority info setup
    Priority pri = Records.newRecord(Priority.class);
    pri.setPriority(requestPriority);

    // computing resource request info setup
    Resource capability = Records.newRecord(Resource.class);
    capability.setMemory(containerMemory);

    // initialize ContainerRequest instance.
    // 1st arg: computing rsc, 4th arg: request priority
    // 2nd arg and 4th arg are used to specify certain racks or nodes to run the container on
    AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null,
            pri);

    return request;
        }

//    ***** NESTED CLASS DECLARATIONS *****

private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {
    // CallbackHandler for RM.
    // Launch execution containers when they are allocated to AM,
    // and reallocate containers if containers fail.
    // assign 'true' to flag 'done' when all containers are successfully completed.
    // the method invoked when the AM recieves an allocated container report from RM
    @Override
    public void onContainersAllocated(List<Container> allocatedContainers) {

        // update value of numAllocatedContainers
        numAllocatedContainers.addAndGet(allocatedContainers.size());

        // for each container in the allocatedContainers report from RM,
        // launch a new thread (with LaunchContainerRunnable Runnable class)
        for (Container allocatedContainer : allocatedContainers) {
            LaunchContainerRunnable runnableLaunchContainer =
                    new LaunchContainerRunnable(allocatedContainer, containerListener);
            // 1st arg: the allocated container instance,
            // 2nd arg: the NMCallbackHandler instance that will listen to the container's status
            Thread launchThread = new Thread(runnableLaunchContainer);

            // launch and start the container on a separate thread to keep
            // the main thread unblocked
            // as all containers may not be allocated at one go.
            launchThreads.add(launchThread);
            launchThread.start();
        }
    }

    // the method invoked when a container finishes task (successful or fail)
    @Override
    public void onContainersCompleted(List<ContainerStatus> completedContainers) {

        // Simple scenario where fault tolerance is not implemented.

        for (ContainerStatus containerStatus : completedContainers) {
            // increment numCompletedContainers
            // for every container in the completedContainers report from RM
            numCompletedContainers.incrementAndGet();
        }

        // set done to 'true' when all containers are complete.
        if (numCompletedContainers.get() == numTotalContainers) {
            done = true;
        }
    }

    @Override
    public void onShutdownRequest() {
        done = true;
    }

    @Override
    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        // don't really need to implement for simple AMs
    }

    @Override
    public float getProgress() {
        // don't really need to implement for simple AMs
        return 0;
    }

    @Override
    public void onError(Throwable e) {
        done = true;
        resourceManager.stop();
    }
}

private class NMCallbackHandler implements NMClientAsync.CallbackHandler {
    // CallbackHandler for NMs
    // don't really need to implement unless for container status monitoring purposes
    // map of containers that the handler keeps track of
    private ConcurrentMap<ContainerId, Container> containers =
            new ConcurrentHashMap<ContainerId, Container>();

    // add a container to monitor
    public void addContainer(ContainerId containerId, Container container) {
        containers.putIfAbsent(containerId, container);
    }

    @Override
    public void onContainerStopped(ContainerId containerId) {
        containers.remove(containerId);
    }

    @Override
    public void onContainerStatusReceived(ContainerId containerId,
                                          ContainerStatus containerStatus) {
        //doesn't really have to be implemented unless for FT purposes
    }

    @Override
    public void onContainerStarted(ContainerId containerId,
                                   Map<String, ByteBuffer> allServiceResponse) {
        //doesn't really have to be implemented unless for FT purposes
    }

    @Override
    public void onStartContainerError(ContainerId containerId, Throwable t) {
        LOG.error("Failed to start Container " + containerId);
        containers.remove(containerId);
    }

    @Override
    public void onGetContainerStatusError(
            ContainerId containerId, Throwable t) {
        //doesn't really have to be implemented unless for FT purposes
    }

    @Override
    public void onStopContainerError(ContainerId containerId, Throwable t) {
        containers.remove(containerId);
    }
}

private class LaunchContainerRunnable implements Runnable {
    // java Runnable implementation for execution container launch threads.
    // in run() method,
    // set up ContainerLaunchContext, add the container to NMCallbackHandler class instance
    // and start the container with the NMClientAsync class instance.
    // Allocated container and NMCallbackHandler handle
    Container container;
    NMCallbackHandler containerListener;

    // constructor takes in the container and handle instances
    public LaunchContainerRunnable(
            Container lcontainer, NMCallbackHandler containerListener) {
        this.container = lcontainer;
        this.containerListener = containerListener;
    }

    // run() function when thread starts
    @Override
    public void run() {

        // set up the ContainerLaunchContext
        ContainerLaunchContext ctx = Records
                .newRecord(ContainerLaunchContext.class);

        //
        ctx.setEnvironment(containerEnv);
       // ctx.setLocalResources(localResources);
        //ctx.setCommands(commands);

        containerListener.addContainer(container.getId(), container);
        nmClientAsync.startContainerAsync(container, ctx);
    }
}
}