package com.bigschool.yarn;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * The general concept is that an 'Application Submission Client' submits an 'Application' to the YARN Resource Manager.
 *
 * The client communicates with the ResourceManager using the 'ApplicationClientProtocol' to first acquire a new 'ApplicationId'
 * if needed via ApplicationClientProtocol#getNewApplication and then submit the 'Application'
 * to be run via ApplicationClientProtocol#submitApplication.
 *
 * As part of the ApplicationClientProtocol#submitApplication call,
 * the client needs to provide sufficient information to the ResourceManager to 'launch' the application's first container
 * i.e. the ApplicationMaster. You need to provide information such as the details about the local files/jars that need to
 * be available for your application to run, the actual command that needs to be executed (with the necessary command line arguments),
 * any Unix environment settings (optional), etc.
 * Effectively, you need to describe the Unix process(es) that needs to be launched for your ApplicationMaster.
 *
 * The YARN ResourceManager will then launch the ApplicationMaster (as specified) on an allocated container.
 *
 * The ApplicationMaster is then expected to communicate with the ResourceManager using the 'ApplicationMasterProtocol'.
 * Firstly, the ApplicationMaster needs to register itself with the ResourceManager. To complete the task assigned to it,
 * the ApplicationMaster can then request for and receive containers via ApplicationMasterProtocol#allocate.
 *
 * After a container is allocated to it, the ApplicationMaster communicates with the NodeManager
 * using ContainerManager#startContainer to launch the container for its task. As part of launching this container,
 * the ApplicationMaster has to specify the ContainerLaunchContext which, similar to the ApplicationSubmissionContext,
 * has the launch information such as command line specification, environment, etc.
 *
 * Once the task is completed,the ApplicationMaster has to signal the ResourceManager of its completion via
 * the ApplicationMasterProtocol#finishApplicationMaster.
 *
 * Meanwhile, the client can monitor the application's status by querying the ResourceManager or by directly querying
 * the ApplicationMaster if it supports such a service. If needed, it can also kill the application
 * via ApplicationClientProtocol#forceKillApplication.
 */
public class AppClient {
    private static final Logger LOG = Logger.getLogger(AppClient.class.getName());
    private Configuration conf;
    private YarnClient yarnClient;

    // Command-line parameters
    private String appname="YARN-Echo";
    private String command="echo";
    private int applicationMasterMem=300;
    private int containerMem=300;
    private int containerCount=4;
    private String hdfsJar="/BigSchoolYarn-1.0.jar";
    private String applicationMasterClassName="com.bigschool.yarn.ApplicationMaster";

    private ApplicationId appId;

    public AppClient() {
        conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        /**
         * Yarn Client's initialization determines the RM's IP address and port.
         * These values are extracted from yarn-site.xml or yarn-default.xml.
         * It also determines the interval by which it should poll for the
         * application's state.
         */
        yarnClient.init(conf);
    }

    public boolean run() throws IOException, YarnException {
        LOG.info("Calling run...");
        yarnClient.start();
        /**
         * [STEP-1]
         * The first step that a client needs to do is to connect to the ResourceManager
         * or to be more specific, the ApplicationsManager (AsM) interface of the ResourceManager.
         */
        YarnConfiguration yarnConf = new YarnConfiguration(conf);
        InetSocketAddress rmAddress = NetUtils.createSocketAddr(yarnConf.get(
                YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS));
        LOG.info("Connecting to ResourceManager at " + rmAddress);

        /**
         * [STEP-2]
         * Once a handle is obtained to the ASM(ApplicationsManager) or ResourceManager,
         * the client needs to request the ResourceManager for a new ApplicationId.
         *
         * The response from the ASM for a new application also contains information
         * about the cluster such as the minimum/maximum resource capabilities of the cluster.
         * This is required so that to ensure that you can correctly set the specifications
         * of the container in which the ApplicationMaster would be launched.
         * Please refer to GetNewApplicationResponse for more details.
         */
        YarnClientApplication app = yarnClient.createApplication();
        GetNewApplicationResponse response = app.getNewApplicationResponse();
        appId = response.getApplicationId();
        LOG.info("Got new ApplicationId=" + appId);

        /**
         * [STEP-3]
         * The main crux of a client is to setup the ApplicationSubmissionContext
         * which defines all the information needed by the ResourceManager to launch the ApplicationMaster.
         * A client needs to set the following into the context:
         *
         * Application Info:        id, name
         * Queue, Priority info:    Queue to which the application will be submitted, the priority to be assigned for the application.
         * User:                    The user submitting the application
         * ContainerLaunchContext:  The information defining the container in which the ApplicationMaster will be launched and run.
         *                          The ContainerLaunchContext, as mentioned previously, defines all the required information needed
         *                          to run the ApplicationMaster such as the local resources (binaries, jars, files etc.),
         *                          security tokens, environment settings (CLASSPATH etc.) and the command to be executed.
         */
        // Create a new ApplicationSubmissionContext
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationId(appId);
        appContext.setApplicationName(this.appname);

        // Set up the container launch context for AM.
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // Define the local resources required
        LocalResource appMasterJar = this.setupAppMasterJar(this.hdfsJar);
        amContainer.setLocalResources(Collections.singletonMap("ae_master.jar", appMasterJar));

        // Set up CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<String, String>();
        setupAppMasterEnv(appMasterEnv);
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(this.applicationMasterMem);
        capability.setVirtualCores(1);

        // setup command to be executed in am container
        amContainer.setCommands(Collections.singletonList(this.getCommand()));

        // put everything together.
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        // Submit application
        /**
         * At this point, the ResourceManager will have accepted the application and in the background,
         * will go through the process of allocating a container with the required specifications and then
         * eventually setting up and launching the ApplicationMaster on the allocated container.
         */
        yarnClient.submitApplication(appContext);

        /**
         * There are multiple ways a client can track progress of the actual task.
         * It can communicate with the ResourceManager and request for a report of the
         * application via ApplicationClientProtocol#getApplicationReport.
         */
        return this.monitorApplication(appId);


    }

    public void init() {
        LOG.setLevel(Level.DEBUG);
        LOG.info("Calling init...");
    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        StringBuilder classPathEnv = new StringBuilder();
        classPathEnv.append(ApplicationConstants.Environment.CLASSPATH.$()).append(File.pathSeparatorChar);
        classPathEnv.append("./*");

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            classPathEnv.append(File.pathSeparatorChar);
            classPathEnv.append(c.trim());
        }

        String envStr = classPathEnv.toString();
        LOG.info("env: " + envStr);
        appMasterEnv.put(ApplicationConstants.Environment.CLASSPATH.name(), envStr);
    }

    private LocalResource setupAppMasterJar(FileStatus status, Path jarHdfsPath) throws IOException {
        LocalResource appMasterJar = Records.newRecord(LocalResource.class);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarHdfsPath));
        appMasterJar.setSize(status.getLen());
        appMasterJar.setTimestamp(status.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.APPLICATION);
        return appMasterJar;
    }

    /**
     * Lets assume the jar we need for our ApplicationMaster is available in
     * HDFS at a certain known path to us and we want to make it available to
     * the ApplicationMaster in the launched container
     */
     private LocalResource setupAppMasterJar(String hdfsPath) throws IOException {
        FileSystem fs = FileSystem.get(this.conf);
        Path dst = new Path(hdfsPath);
        // must use fully qualified path name. Otherwise, node manager gets angry.
        dst = fs.makeQualified(dst);
        return this.setupAppMasterJar(fs.getFileStatus(dst), dst);
    }

    private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {
        boolean r = false;
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                r = false;
                break;
            }

            ApplicationReport report = yarnClient.getApplicationReport(appId);
            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus status = report.getFinalApplicationStatus();

            if (state == YarnApplicationState.FINISHED) {
                if (status == FinalApplicationStatus.SUCCEEDED) {
                    LOG.info("Completed sucessfully.");
                    r = true;
                    break;
                } else {
                    LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue=" + status.toString());
                    r = false;
                    break;
                }
            } else if (state == YarnApplicationState.KILLED || state == YarnApplicationState.FAILED) {
                LOG.info("Application errored out. YarnState=" + state.toString() + ", finalStatue=" + status.toString());
                r = false;
                break;
            }
        }// while
        return r;
    }

    /**
     * Construct the command to be executed on the launched container
     * @return command
     */
    private String getCommand() {
        StringBuilder sb = new StringBuilder();
        sb.append(ApplicationConstants.Environment.JAVA_HOME.$()).append("/bin/java").append(" ");
        sb.append("-Xmx").append(this.applicationMasterMem).append("M").append(" ");
        sb.append(this.applicationMasterClassName).append(" ");
        sb.append("--").append("container_mem").append(" ").append(this.containerMem).append(" ");
        sb.append("--").append("container_cnt").append(" ").append(this.containerCount).append(" ");
        sb.append("--").append("command").append(" '").append(StringEscapeUtils.escapeJava(this.command)).append("' ");

        sb.append("1> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout").append(" ");
        sb.append("2> ").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr");
        String r = sb.toString();
        LOG.info("ApplicationConstants.getCommand() : " + r);
        return r;
    }

    public static void main(String[] args) throws IOException, YarnException {
        LOG.info("Calling main...");
        AppClient client = new AppClient();
        boolean r = false;
        client.init();
        r = client.run();
        if (r) {
            System.exit(0);
        }
        System.exit(2);

    }
}


