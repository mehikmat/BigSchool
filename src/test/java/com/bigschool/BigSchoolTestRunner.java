package com.bigschool;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class BigSchoolTestRunner{
    public static void main(String[] args) {

        Result result = JUnitCore.runClasses(
                BigSchoolMapperTest.class,
                BigSchoolReduceTest.class,
                CombinerPartitionerTest.class);

        for (Failure failure : result.getFailures()) {
            System.out.println(failure.toString());
        }
    }
}

