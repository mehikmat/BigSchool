package com.bigschool.comparator;

import org.apache.hadoop.io.WritableComparator;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class CustomKeyOrderComparator extends WritableComparator {

    @Override
    public int compare(Object a, Object b) {
        return (b.toString().compareTo(a.toString()));
    }
}
