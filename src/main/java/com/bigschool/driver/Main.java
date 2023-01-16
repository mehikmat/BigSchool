package com.bigschool.driver;

import com.bigschool.driver.apllications.DistributedCacheApplication;
import com.bigschool.driver.apllications.StudentInfoIndexApplication;
import com.bigschool.driver.apllications.StudentInfoProcessApplication;

/**
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

        if (args[0].equalsIgnoreCase("distributed-cache"))
            System.exit(new DistributedCacheApplication().runApplication(args));
    }
}
