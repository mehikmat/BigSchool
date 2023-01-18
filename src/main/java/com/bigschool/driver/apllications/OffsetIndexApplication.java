package com.bigschool.driver.apllications;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Date;
import java.util.Scanner;

import static com.bigschool.driver.apllications.DistributedCacheApplication.MASTER_PATH;
import static com.bigschool.driver.apllications.DistributedCacheApplication.OFFSET_PATH;

/**
 * Sorting Comparators
 * ==================
 * Useful link for secondary sort implementation by means of composite primary key {natural_key,secondary_key}
 * http://vangjee.wordpress.com/2012/03/20/secondary-sorting-aka-sorting-values-in-hadoops-mapreduce-programming-paradigm/
 * Steps:
 * 1. Write custom partitioner and use natural key for partitioning (group by key)
 * (multiple unique keys may go to same reducer). So
 * 2. Write custom natural key grouping comparator (primary sort)
 * 3. Write custom composite key comparator (secondary sort)
 * int result = k1.getPrimaryKey().compareTo(k2.getPrimaryKey());
 * if(0 == result) {
 * result = -1* k1.getSecondaryKey().compareTo(k2.getSecondaryKey());
 * }
 * <p>
 * Grouping Comparator
 * ===================
 * Reducer Instance vs reduce method:
 * One JVM is created per Reduce task and each of these has a single instance
 * of the Reducer class.This is Reducer instance(I call it Reducer from now).
 * Within each Reducer, reduce method is called multiple times depending on
 * 'key grouping'.Each time reduce is called, 'valuein' has a list of map output
 * values grouped by the key you define in 'grouping comparator'.By default,
 * grouping comparator uses the entire map output key.
 * <p>
 * Example
 * Input:
 * symbol time price
 * a 1 10
 * a 2 20
 * b 3 30
 * Map output: create composite key\values like so symbol-time time-price
 * a-1 1-10
 * a-2 2-20
 * b-3 3-30
 * The Partitioner: will route the a-1 and a-2 keys to the same reducer despite the keys being different.
 * It will also route the b-3 to a separate reducer.
 * <p>
 * GroupComparator: once the composites key\value arrive at the reducer instead of the reducer getting
 * (a-1,{1-10})
 * (a-2,{2-20})
 * the above will happen due to the unique key values following composition.
 * the group comparator will ensure the reducer gets:
 * (a,{1-10,2-20})
 * [[In a single reduce method call.]]
 *
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class OffsetIndexApplication implements HadoopApplication {

    public void createIndexFile(String fileName) throws IOException {
        // creates an object of Scanner
        Scanner input = new Scanner(Files.newInputStream(Paths.get(MASTER_PATH)));

        Integer offset = 0;
        while (input.hasNext()) {
            String line = input.nextLine();
            String[] parts = line.split("\\|");

            offset += line.length() + 1;
            Files.write(Paths.get(OFFSET_PATH), (parts[0] + "|" + offset + "\n").getBytes(), StandardOpenOption.APPEND);
        }
        // closes the scanner
        input.close();
    }

    @Override
    public int runApplication(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // record start time
        Date startTime = new Date();
        System.out.println("Job started: " + startTime);

        createIndexFile(MASTER_PATH);

        Date endTime = new Date();
        System.out.println("Job ended: " + endTime);
        System.out.println("The job took=====================>>>>>> " +
                (endTime.getTime() - startTime.getTime()) / 1000 + " seconds.");

        // return status
        return 0;
    }
}
