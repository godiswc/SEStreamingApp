package com.cloudera.hbase.query;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;


/**
 * Created by root on 6/11/17.
 */
public class SECDataPointHBaseQuery implements HBaseQuery{

    private static final byte[] cf = Bytes.toBytes("cf");
    private static final byte[] q = Bytes.toBytes("v");

    public SECDataPointHBaseQuery(){

    }

    @Override
    public void query(HBaseQueryRequest request, HTable table) {
        if(request instanceof SECDataPointQueryRequest) {
            SECDataPointQueryRequest r = (SECDataPointQueryRequest) request;
            String startRow = r.getPointCode()+"|"+r.getFactoryCode()+"|"+r.getGroupCode()+"|"+r.getStartTime();
            String stopRow = r.getPointCode()+"|"+r.getFactoryCode()+"|"+r.getGroupCode()+"|"+r.getEndTime();
            //Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(stopRow));
            Scan scan = new Scan();
            scan.setStartRow(Bytes.toBytes(startRow));
            scan.setStopRow(Bytes.toBytes(stopRow));
            scan.addColumn(cf,q);
            try {
                ResultScanner rs = table.getScanner(scan);
                System.out.println(rs.iterator().hasNext());
                for(Result result:rs){
                    int type = r.getType();
                    switch (type){
                        case 0:
                            System.out.println("Value for:" + request.toString() + "  " +Bytes.toFloat(result.getValue(cf,q)));
                            break;
                        case 1:
                            System.out.println("Value for:" + request.toString() + "  " +Bytes.toInt(result.getValue(cf,q)));
                            break;
                        case 2:
                            System.out.println("Value for:" + request.toString() + "  " +Bytes.toDouble(result.getValue(cf,q)));
                            break;
                        case 3:
                            System.out.println("Value for:" + request.toString() + "  " +Bytes.toLong(result.getValue(cf,q)));
                            break;
                        case 4:
                            System.out.println("Value for:" + request.toString() + "  " +Bytes.toBoolean(result.getValue(cf,q)));
                            break;
                        default:
                            System.out.println("No Matching value type");
                    }
                }
                rs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}


