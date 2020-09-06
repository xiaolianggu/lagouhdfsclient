package com.gu.hadoop;
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
/** 
 * 描述：就是为了统计 每个分区中的数据的条数
 */
public class IndexNumerMR_2 {
 
	public static void main(String[] args) throws Exception {
 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "IndexNumerMR_2");
		job.setJarByClass(IndexNumerMR_2.class);
 
		job.setMapperClass(IndexNumerMR_2Mapper.class);
		job.setReducerClass(IndexNumerMR_2Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
 
		Path inputPath = new Path("E:/hadoop/input");
		Path outputPath = new Path("E:/hadoop/output");
		FileInputFormat.addInputPath(job, inputPath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
 
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
 
	/**
	 * Text, LongWritable
	 * 
	 * key :  对应的分区名
	 * value ： 对应分区中的一个值   ===  1
	 */
	public static class IndexNumerMR_2Mapper extends Mapper<LongWritable, Text, Text, LongWritable> {
 
		private Text keyOut = new Text();
		private LongWritable ONE = new LongWritable(1);
 
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
 
			String string = value.toString();
			long outValue = Long.parseLong(string);
 
			/**
			 *  key ： 对应的分区的名
			 *  value： 该分区中的一个数值
			 */
			if (outValue < 100) {
				keyOut.set("part-r-00000");
			} else if (outValue >= 100 && outValue <= 999) {
				keyOut.set("part-r-00001");
			} else {
				keyOut.set("part-r-00002");
			}
 
			context.write(keyOut, ONE);
 
		}
	}
 
	public static class IndexNumerMR_2Reducer extends Reducer<Text, LongWritable, Text, LongWritable> {
 
		private LongWritable valueOut = new LongWritable();
 
		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) 
				throws IOException, InterruptedException {
 
			int sum = 0;
			for (LongWritable lw : values) {
				sum += lw.get();
			}
 
			valueOut.set(sum);
 
			context.write(key, valueOut);
		}
 
	}
}