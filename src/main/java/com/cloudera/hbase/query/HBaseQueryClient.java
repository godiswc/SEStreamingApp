package com.cloudera.hbase.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by root on 6/11/17.
 */
public class HBaseQueryClient {

    private HBaseQuery query;
    private HBaseQueryRequest request;
    private String tableName;
    private Map<String,String> typeMap;


    public HBaseQueryClient(HBaseQueryRequest request,String tableName){
        this.request = request;
        this.tableName = tableName;
        this.query = new SECDataPointHBaseQuery();
        typeMap = new HashMap<>();
        loadTypeMap();
    }


    public void query(){
        Configuration conf = HBaseConfiguration.create();
        conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));
        //conf.set("hbase.zookeeper.quorum","10.84.3.18:2181");
        if(request instanceof SECDataPointQueryRequest){
            ((SECDataPointQueryRequest) request).setType(getColumnType(((SECDataPointQueryRequest) request).getPointCode()));
        }
        try {
            HTable table = new HTable(conf,tableName);
            query.query(request,table);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void loadTypeMap(){
        try {
            BufferedReader reader = new BufferedReader(new FileReader("./conf/type.txt"));
            String line;
            while((line=reader.readLine())!=null){
                String kv[] = line.split("\\t");
                //System.out.println(kv[0]+kv[1]);
                typeMap.put(kv[0],kv[1]);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public int getColumnType(String code){
        if(typeMap!=null){
            return Integer.parseInt(typeMap.get(code));
        }

        return -1;
    }



    public static void main(String[] args){
        String table = args[0];
        String factoryCode = args[1];
        String groupCode = args[2];
        String pointCode = args[3];
        String startTime = args[4];
        String endTime = args[5];



        SECDataPointQueryRequest request = new SECDataPointQueryRequest(pointCode,factoryCode,groupCode,Long.parseLong(startTime),Long.parseLong(endTime));
        HBaseQueryClient client = new HBaseQueryClient(request,table);

        client.query();

    }

}
