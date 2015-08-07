package com.bigschool;

import cascading.cascade.Cascade;
import cascading.cascade.CascadeConnector;
import cascading.flow.Flow;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.FirstBy;
import cascading.scheme.hadoop.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import redis.clients.jedis.Jedis;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author Hikmat Dhamee
 * @email me.hemant.available@gmail.com
 */
public class Main {

    public void run() {
        Tap src1 = new Hfs(new TextDelimited(new Fields("a","b","c","d"),";"), "input/input.txt", SinkMode.KEEP);
        Tap snk1 = new Hfs(new TextDelimited(new Fields("a","b","c","d"),";"), "output1", SinkMode.REPLACE);

        Pipe pipe1 = new Pipe("copy1");
        pipe1 = new Each(pipe1,new FilterFunction(),Fields.RESULTS);
        pipe1 = new FirstBy(pipe1,new Fields("groupFields"),new Fields("firstFields"));

        Flow flow1 = new HadoopFlowConnector().connect(src1, snk1, pipe1);

        CascadeConnector connector = new CascadeConnector();
        Cascade cascade = connector.connect( flow1);
        cascade.complete();

    }

    public void uploadToRedis(String filePath){
        Jedis jedis = new Jedis("localhost");
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader( new FileInputStream(filePath)));
            String line;

            System.out.println("Uploading..........");
            while ((line = reader.readLine()) != null) {
                final String split[] = line.split(";");
                jedis.set(split[0], split[1]);
            }
        } catch (IOException e) {
            throw new RuntimeException(e.getLocalizedMessage());
        }
    }

    public static void main(String[] args) {
        if (args[0].equals("upload_to_redis")){
            new Main().uploadToRedis(args[1]);
        }else if (args[0].equals("use_redis_cache")){
            new Main().run();
        }
    }
}
