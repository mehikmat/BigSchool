package com.bigschool.util;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class ResourceUtils {
    public static Properties loadProperties(String fileName) {
        Properties prop = new Properties();
        if(fileName==null||fileName.isEmpty()){
            return prop;
        }
        try {
            if (fileName.startsWith("classPath:")) {
                InputStream stream = ResourceUtils.class.getClassLoader().
                        getResourceAsStream(fileName.substring("classPath:".length()));
                prop.load(stream);

            } else {
                FileInputStream stream = new FileInputStream(fileName);
                DataInputStream in = new DataInputStream(stream);
                prop.load(in);
            }
            return prop;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
