package com.bigschool.driver;

import com.bigschool.driver.apllications.StudentInfoIndexApplication;
import com.bigschool.driver.apllications.StudentInfoProcessApplication;

/**
 * MapReduce Operations showcased here are
 *
 * ===============================================
 *  MapReduce Operators
 *  --------------------
 *  1. Mapper
 *  2. Reducer
 *  3. Combiner
 *  4. Partitioner
 *  5. Sorting [GropingComparator and KeyComparator]
 * 
 * ==================================================
 * MapReduce Framework
 * -------------------
 *  Shuffle:The process of moving map outputs to the reducers is known as shuffling
 *  Sort: Each reduce task is responsible for reducing the values associated with several intermediate keys.
 *        The set of intermediate keys on a single node is automatically sorted by Hadoop before they are
 *        presented to the Reducer.
 *  Speculative Execution: As most of the tasks in a job are coming to a close(nearly 95%),
 *        the Hadoop platform will schedule redundant copies of the remaining tasks across
 *        several nodes which do not have other work to perform. This process is known as
 *        speculative execution. When tasks complete, they announce this fact to the JobTracker.
 *        Whichever copy of a task finishes first becomes the definitive copy.
 *        If other copies were executing speculatively, Hadoop tells the TaskTrackers to abandon
 *        the tasks and discard their outputs.By default it is true.
 * 
 *  Common MapReduce Operations
 *  ---------------------------
 *  1. Filtering or Grepping
 *  2. Parsing, Conversion
 *  3. Counting, Summing
 *  4. Binning, Collating
 *  5. Distributed Tasks
 *  6. Simple Total Sorting
 *  7. Chained Jobs
 *  ==============================================
 *
 * Advanced MapReduce Operations
 * -----------------------------
 *  1. GroupBy
 *  2. Distinct
 *  3. Secondary Sort
 *  4. CoGroup/Join
 *  5. Distributed Total Sort
 *  6. Distributed Cache
 *  7. Reading from HDFS programmetically
 *
 *  Very Advanced Operations
 *  ------------------------
 *  1. Classification
 *  2. Clustering
 *  3. Regression
 *  4. Dimension Reduction
 *  5. Evolutionary Algorithms
 *
 *  Combiner
 *  --------
 *   The best part of all is that we do not need to write any additional code
 *   to take advantage of this! If a reduce function is both commutative
 *   and associative, then it can be used as a Combiner as well.
 *
 *   Partitioner
 *   -----------
 *   The key and value are the intermediate key and value produced by the map function.
 *   The numReduceTasks is the number of reducers used in the MapReduce program
 *   and it is specified in the driver program.
 *   It is possible to have empty partitions with no data (when no of partition is less then no of reducer).
 *   We do the assigned partition number modulo numReduceTasks to avoid illegal partitions
 *   if the system has a lesser number of possible reducers than the assigned partition number.
 *
 *   The partitioning phase takes place after the map/combine phase and before the reduce phase.
 *   The number of partitions is equal to the number of reducers.
 *   The data gets partitioned across the reducers according to the partitioning function
 *
 *   Sequence File
 *   --------------
 *    SequenceFiles are flat files consisting of binary key/value pairs.
 *    SequenceFile provides SequenceFile.Writer, SequenceFile.Reader and
 *    SequenceFile.Sorter classes for writing, reading and sorting respectively.
 *
 *  Counting and Summing
 *  --------------------
 *  For instance, there is a log file where each record contains a response time and
 *  it is required to calculate an average response time.
 *  Applications: Log Analysis, Data Querying
 *
 *  Binning and Collating
 *  ---------------------
 *  IN>> word : list of page numbers containing word
 *  Applications: Inverted Indexes, ETL,Max,Min
 *
 *  Filtering or Grepping
 *  ---------------------
 *  Applications: Log Analysis, Data Querying, ETL, Data Validation
 *
 *  Distributed Task
 *  ----------------
 *  There is a large computational problem that can be divided into multiple parts
 *  and results from all parts can be combined together to obtain a final result.
 *  Applications: Physical and Engineering Simulations, Numerical Analysis, Performance Testing
 *
 *   Chaining jobs
 *   --------------
 Method 1:
 First create the JobConf object "job1" for the first job and set all the parameters with "input"
 as inputdirectory and "temp" as output directory. Execute this job: JobClient.run(job1).
 Immediately below it, create the JobConf object "job2" for the second job and set all the
 parameters with "temp" as inputdirectory and "output" as output directory.
 Finally execute second job: JobClient.run(job2).

 Method 2:
 Create two JobConf objects and set all the parameters in them just like (1) except that
 you don't use JobClient.run.
 Then create two Job objects with jobconfs as parameters:
 Job job1=new Job(jobconf1); Job job2=new Job(jobconf2);
 Using the jobControl object, you specify the job dependencies and then run the jobs:
 JobControl jbcntrl=new JobControl("jbcntrl");
 jbcntrl.addJob(job1);
 jbcntrl.addJob(job2);
 job2.addDependingJob(job1);
 jbcntrl.run();
 *
 *
 *  Distinct Values (Unique Items Counting)
 *  --------------------------------------- *
 *
 * Help Take from: https://highlyscalable.wordpress.com/2012/02/01/mapreduce-patterns/
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("Usage: $HADOOP_HOME/bin/yarn jar job_name [input] [output]");
            System.exit(-1);
        }

        if (args[0].equalsIgnoreCase("student-process"))
            System.exit(new StudentInfoProcessApplication().runApplication(args));

        if (args[0].equalsIgnoreCase("student-index"))
            System.exit(new StudentInfoIndexApplication().runApplication(args));
    }
}
