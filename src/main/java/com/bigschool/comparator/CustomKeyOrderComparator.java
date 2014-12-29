package com.bigschool.comparator;

import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import sun.plugin.navig.motif.OJIPlugin;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class CustomKeyOrderComparator extends WritableComparator {

    @Override
    public int compare(Object a, Object b) {
        System.out.println(">>>>>>>>>>"+b.toString()+a.toString());
        return (b.toString().compareTo(a.toString()));
    }
}
