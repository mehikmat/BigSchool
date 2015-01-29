package com.bigschool.indexing;

import com.bigschool.context.AppContext;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public interface AlgorithmLifeCycle {
    /**
     * Sets up environment for execution.
     * The set up may include initialization of formats for the job, parsing the configuration
     * file and so on depending upon the unit of work dedicated to the implementation.
     *
     * @param context
     */
    public void setUp(AppContext context);

    /**
     * Cleans up the parameters and resources allocated by the setup method.
     * @param context
     */
    public void cleanUp(AppContext context);
}
