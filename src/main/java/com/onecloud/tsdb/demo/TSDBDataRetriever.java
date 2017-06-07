package com.onecloud.tsdb.demo;

import com.cloudera.common.SEStreamingConstants;
import onecloud.plantpower.database.driver.protobuf.Driver;
import onecloud.plantpower.database.driver.protobuf.TSDBClient;
import onecloud.plantpower.database.driver.protobuf.TSDBStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;


/**
 * Created by root on 5/31/17.
 */
public class TSDBDataRetriever {

    private static final Logger logger = LoggerFactory.getLogger(TSDBDataRetriever.class);

    public final static String TSDB_HOST = "10.0.43.24";
    public final static int TSDB_PORT = 4343;

    private TSDBClient client;
    private long lastEndTime;
    private Driver.FindDataPointRequest.Builder builder;

    public TSDBDataRetriever(){
        try {
            client = new TSDBClient(TSDB_HOST,TSDB_PORT);
            lastEndTime = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TSDBDataRetriever(String host, int port){
        try {
            client = new TSDBClient(host,port);
            lastEndTime = 0;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Driver.FindDataPointResponse retrieve(Properties prop) throws  IOException {
        Driver.FindDataPointRequest request = requestBuild(prop);
        Driver.FindDataPointResponse response = client.findDataPoint(request);

        if (response.getStatus() && response.getDataPointsCount() > 0) {
            logger.info("Find data: \t success");
            System.out.println("Find data: \t success");
            for(TSDBStruct.DataPoint dp:response.getDataPointsList()){
                for(TSDBStruct.PointValue pv: dp.getValuesList()){
                    System.out.println("Time "+pv.getTimestamp()+" value "+pv.getFloatValue());
                }
            }
        }
        else
            logger.info("Find data: \t fail");

        return response;

    }

    public Driver.FindDataPointRequest requestBuild(Properties prop){
        long now = System.currentTimeMillis() / 1000 * 1000;
        if(lastEndTime == 0){
            lastEndTime = now - 6000*1000;
        }

        if(builder == null) {
            String points = prop.getProperty(SEStreamingConstants.POINTS_LIST);
            String[] points_list = points.split(SEStreamingConstants.SEP);

            builder = Driver.FindDataPointRequest.newBuilder();

            for (String s : points_list) {
                String[] point = s.split(SEStreamingConstants.DELIMITER);
                builder.addPoints(TSDBClient.newPoint(point[0], point[1], point[2]));
            }
        }

        Driver.FindDataPointRequest request = builder.setInterpolation(true).setDownsampler(TSDBStruct.Downsampler.AVG).setInterval(1000)
                .setStartTimestamp(lastEndTime).setEndTimestamp(now).build();
        lastEndTime = now;
        return request;
    }
}
