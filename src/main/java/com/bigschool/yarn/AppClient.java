package com.bigschool.yarn;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import java.io.IOException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: hikmat
 * Date: 3/15/13
 * Time: 1:05 PM
 * To change this template use File | Settings | File Templates.
*/
public class AppClient {
    // field declarations

    // instances that you will use for basic configuration
    private Configuration conf;     // cluster configuration instance for client
    private YarnClient yarnClient;  // YARN client instance

    // info that needs to be passed to AsM (using ApplicationSubmissionContext)
    private String appName;
    private int amPriority;
    private String amQueue;
    private int amMemory;         // amount of memory allocation for AM
    private String appMasterJar;  // .jar containing the AM code
   private final String appMasterMainClass;  // class name of AM code
    private Options opts;

    public AppClient() {
        this(new YarnConfiguration()); // the actual constructor that is invoked.
    }
    public AppClient(Configuration conf) {
        this.conf = conf; // conf is initialized by the first constructor
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf); // initialize cluster configurations of yarnClient with conf
        opts = new Options();
        opts.addOption("appname", true, "Application Name");
        opts.addOption("priority", true, "Application Priority");
        opts.addOption("queue", true, "RM Queue in which this application is to be submitted");
        opts.addOption("master_memory", true, "Amount of memory in MB to be requested to run the application master");
        opts.addOption("jar", true, "Jar file containing the application master");
        appMasterMainClass="com.bigschool.yarn.ApplicationMaster";
        // (other options for your customized YARN application)
    }


    // class method declarations

    public static void main(String[] args) throws IOException, YarnException, ParseException {
        // initialize and run client

            AppClient client=new AppClient();

            client.init(args); // initialize client class fields
                        // with arguments
            client.run();        // start running the client

    }

    public void init(String[] args) throws ParseException {
        // initialize Client class fields with argument values provided via command line
        // command line parser that returns values of options
        CommandLine cliParser = new GnuParser().parse(opts, args);

        appName = cliParser.getOptionValue("appname");
        amPriority = Integer.parseInt(cliParser.getOptionValue("priority"));
        amQueue = cliParser.getOptionValue("queue");
        amMemory = Integer.parseInt(cliParser.getOptionValue("master_memory"));
        appMasterJar = cliParser.getOptionValue("jar");

        // (other initializations for your customized YARN application)
    }

    public void run() throws IOException, YarnException {
        // main run function of client.
        // get Application ID from AsM,
        // set up ApplicationSubmissionContext and ContainerLaunchContext,
        // and finally submit the application.
        yarnClient.start();

        // YarnClientApplication is the API used between Client and RM (named ClientRMprotocol in alpha versions)
        YarnClientApplication app = yarnClient.createApplication();

        // appContext: ApplicationSubmissionContext instance
        // amContainer: ContainerLaunchContext instance
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);

        // set the appID (ID is assigned upon getApplicationSubmissionContext()).
        // the instance appID contains meta info about the application, such as execution status.
        // can be used for customized purposes
        ApplicationId appId = appContext.getApplicationId();

        // prepare .jar file (localResources instance) for amContainer
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        LocalResource amJarRsrc = Records.newRecord(LocalResource.class); // amJarRsrc: instance used for the jar file
        amJarRsrc.setResource(ConverterUtils.getYarnUrlFromPath(new Path("/"))); // assuming that the jar is in HDFS
        localResources.put("AppMaster.jar", amJarRsrc);

        // prepare CLASSPATH to jar in HDFS (env instance) for amContainer
        Map<String, String> env = new HashMap<String, String>();
        env.put("CLASSPATH", "Path to jar in HDFS");

        // prepare launch command (commands instance) for amContainer
        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$() + "/bin/java");
        vargs.add("-Xmx" + amMemory + "m");
        vargs.add(appMasterMainClass);
        vargs.add("--priority " + String.valueOf(amPriority));

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        List<String> commands = new ArrayList<String>(); // convert command to string
        commands.add(command.toString());

        // prepare amMemory info (converted to capability instance) for appContext
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(amMemory);

        // prepare amPriority info (converted to pri instance) for appContext
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(amPriority);

        // set up amContainer (ContainerLaunchContext for the AM container)
        // with localResources, env, and commands
        amContainer.setLocalResources(localResources);
        amContainer.setEnvironment(env);
        amContainer.setCommands(commands);

        // set up appContext (ApplicationSubmissionContext for app. submission to AsM)
        // with appName, capability, pri, amQueue, and amContainer
        appContext.setApplicationName(appName);
        appContext.setResource(capability);
        appContext.setPriority(pri);
        appContext.setQueue(amQueue);
        appContext.setAMContainerSpec(amContainer);

        // finally, submit application with appContext
        yarnClient.submitApplication(appContext);
    }
}


