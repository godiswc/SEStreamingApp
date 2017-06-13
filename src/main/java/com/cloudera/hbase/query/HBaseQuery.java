package com.cloudera.hbase.query;

import org.apache.hadoop.hbase.client.HTable;

/**
 * Created by root on 6/11/17.
 */
public interface HBaseQuery {

    public void query(HBaseQueryRequest request,HTable table);

}
