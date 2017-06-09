package com.onecloud.tsdb.demo;

import com.cloudera.common.SEStreamingConstants;
import onecloud.plantpower.database.driver.protobuf.Driver;
import onecloud.plantpower.database.driver.protobuf.TSDBClient;
import onecloud.plantpower.database.driver.protobuf.TSDBStruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
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
    private Map<String,Long> timestampMap;
    //private int index =0;


    private Driver.FindDataPointRequest.Builder builder;

    public TSDBDataRetriever(){
        try {
            client = new TSDBClient(TSDB_HOST,TSDB_PORT);
            lastEndTime = 0L;
            timestampMap = new HashMap<String,Long>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public TSDBDataRetriever(String host, int port){
        try {
            client = new TSDBClient(host,port);
            lastEndTime = 0L;
            timestampMap = new HashMap<String,Long>();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public Driver.FindDataPointResponse retrieve(Properties prop) throws  IOException {
        Driver.FindDataPointRequest request = requestBuild(prop);
        Driver.FindDataPointResponse response = client.findDataPoint(request);

        if (response.getStatus() && response.getDataPointsCount() > 0) {
            //logger.info("Find data: \t success");
            //System.out.println("Find data: \t success");

//            List<TSDBStruct.DataPoint> list = response.getDataPointsList();
//            for(int i=0;i<list.size();i++){
//                TSDBStruct.DataPoint dp = list.get(i);
//                List<TSDBStruct.PointValue> valueList = dp.getValuesList();
//                if(valueList.size()>=1) {
//                    TSDBStruct.PointValue pv = valueList.get(valueList.size() - 1);
//                    long ts = pv.getTimestamp();
//
//                    long lts = 0L;
//                    if(timestampMap.get(dp.getPoint().getCode())!=null){
//                        lts = timestampMap.get(dp.getPoint().getCode());
//                    }
//                    if(lts!=0L){
//                        if(ts < lts){
//                            response.getDataPointsList().remove(i);
//                        }
//                        timestampMap.put(dp.getPoint().getCode(),ts);
//                    }else{
//                        timestampMap.put(dp.getPoint().getCode(),ts);
//                    }
//                }
//            }

            for(TSDBStruct.DataPoint dp:response.getDataPointsList()){
                for(TSDBStruct.PointValue pv: dp.getValuesList()){
                    if(pv.hasBoolValue()) System.out.println("boolean");
                    if(pv.hasDoubleValue()) System.out.println("double");
                    if(pv.hasFloatValue()) System.out.println("float");
                    if(pv.hasIntValue()) System.out.println("int");
                    if(pv.hasLongValue()) System.out.println("long");
                    //System.out.println("===================="+index);
                    System.out.println("Time "+pv.getTimestamp()+" value "+pv.getFloatValue());
                }
            }
        }
        else
            logger.info("Find data: \t fail");
        //index++;
        return response;

    }

    public Driver.FindDataPointRequest requestBuild(Properties prop){
        long now = System.currentTimeMillis() / 1000 * 1000;
        long interval = Long.parseLong(prop.getProperty(SEStreamingConstants.FETCH_TIME_OFFSET));
        if(lastEndTime == 0){
            lastEndTime = now - interval;
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
