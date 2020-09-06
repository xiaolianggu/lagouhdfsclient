package com.gu.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseClient {


    public static Configuration conf;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum","node1");
        conf.set("hbase.zookeeper.property.clientPort","2181");
        try {
			conn = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
    }

    public static void deleteByRowKey(String tableName, String rowKey,String family, String column) throws IOException {

    	Table table = conn.getTable(TableName.valueOf(tableName));
    	Delete delete = new Delete(Bytes.toBytes(rowKey));
    	//删除指定列
    	delete.addColumns(Bytes.toBytes(family),Bytes.toBytes(column));
    	table.delete(delete);
    }
    public static void main(String[] args) throws IOException {
    	String rowKey1 = "uuid1";
    	String rowKey2 = "uuid2";
    	deleteByRowKey("users","uuid1","friends","uuid2");
    	deleteByRowKey("users","uuid2","friends","uuid1");
    }

    
}
