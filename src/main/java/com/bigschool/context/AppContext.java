package com.bigschool.context;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class AppContext {
    public Mapper.Context mapContext;
    public Reducer.Context reduceContext;
}
