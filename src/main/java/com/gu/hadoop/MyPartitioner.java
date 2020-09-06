package com.gu.hadoop;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
 
/**
 *  KEY, VALUE 就是mapper组件的输出 key-value的类型
 */
public class MyPartitioner extends Partitioner<LongWritable, NullWritable>{
 
	@Override
	public int getPartition(LongWritable key, NullWritable value, int numPartitions) {
		
		// 怎么制定分区规则？
		
		if(key.get() < 100){
			return 0;
		}else if(key.get() >=100  && key.get() <= 999){
			return 1;
		}else{
			return 2;
		}
	}
 
}