package com.bigschool.driver.apllications;

import java.io.IOException;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public interface HadoopApplication {
    public int runApplication(String[] args) throws IOException, ClassNotFoundException, InterruptedException;
}
