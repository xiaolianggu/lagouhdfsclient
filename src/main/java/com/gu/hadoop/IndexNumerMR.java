package com.gu.hadoop;
import java.io.IOException;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
/**
 * 当前这个MR是为了实现 全局排序， 而且每个数值还要加序号
 */
public class IndexNumerMR {
 
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "IndexNumerMR");
		job.setJarByClass(IndexNumerMR.class);
 
		job.setMapperClass(IndexNumerMRMapper.class);
		job.setReducerClass(IndexNumerMRReducer.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		
		
		job.setNumReduceTasks(3);
		job.setPartitionerClass(MyPartitioner.class);
 
		Path inputPath = new Path("E:\\hadoop\\input");
		Path outputPath = new Path("E:\\hadoop\\output");
		FileInputFormat.addInputPath(job, inputPath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
 
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
 
	public static class IndexNumerMRMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {
 
		private LongWritable keyOut = new LongWritable();
		
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
             System.out.print("11");
			long outKey = Long.parseLong(value.toString());
			//逐行读取数组中的数据，然后直接输出，根据自己定义的partitioner进行分区。
			keyOut.set(outKey);
			
			context.write(keyOut, NullWritable.get());
		}
	}
 
	public static class IndexNumerMRReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
 
		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> values, Context context) 
				throws IOException, InterruptedException {
 
			
			for(NullWritable nvl : values){
				//直接输出
				context.write(key, nvl);
			}
		}
 
	}
}