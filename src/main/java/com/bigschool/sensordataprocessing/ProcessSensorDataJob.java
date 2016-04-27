package com.bigschool.sensordataprocessing;

/**
 * Created by hdhamee on 4/27/16.
 */
public class ProcessSensorDataJob {
    public static void main(String[] args) {
        // Join hvac.csv and buildings.csv data
        /*
        create table if not exists hvac_buildings as select h.*, b.* from buildings as b join hvac as h on b.buildingid = h.buildingid;
        */


        // Analytics/refine
       /*
                       CREATE TABLE hvac_report as
                        select *,
                        targettemp - actualtemp as temp_diff,
                        IF((targettemp - actualtemp) > 5, 'COLD',IF((targettemp - actualtemp) < -5, 'HOT', 'NORMAL')) AS temprange,
                        IF((targettemp - actualtemp) > 5, '2',IF((targettemp - actualtemp) < -5, '1',0)) AS extremetemp
                        from hvac_buildings;
        */

    }
}
