package com.gu.hadoop;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
/**
 * 读取第一个mapreduce程序有3个mapTask
 * 然后在每个mapTask节点都把第二个程序的结果都给加载内存中。
 * 事实上就是一个mapjoin的实现。
 */
public class IndexNumerMR_3 {
 
	public static void main(String[] args) throws Exception {
 
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Job job = Job.getInstance(conf, "IndexNumerMR_3");
		job.setJarByClass(IndexNumerMR_3.class);
 
		job.setMapperClass(IndexNumerMR_3Mapper.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
 
		job.setNumReduceTasks(0);
 
		//本地测试使用
		job.addCacheFile(new URI("file:/E:/hadoop/output/part-r-00000"));
 
		Path inputPath = new Path("E:/hadoop/output");
		Path outputPath = new Path("E:/hadoop/output1");
		FileInputFormat.addInputPath(job, inputPath);
		if (fs.exists(outputPath)) {
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
 
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
 
	public static class IndexNumerMR_3Mapper extends Mapper<LongWritable, Text, LongWritable, Text> {
 
		private LongWritable keyOut = new LongWritable();
 
		/**
		 * 当前这个mapTask的编号起点 
		 */
		private Long indexStart = 1l;
 
		private Map<String, Long> ptnCountMap = new HashMap<>();
 
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
 
			/**
			 * 就是为了加载
			 * part-r-00000	9
			   part-r-00001	3
			   part-r-00002	2
			         到ptnCountMap中
			 */
			//走集群使用
			//Path[] localCacheFiles = context.getLocalCacheFiles();
			//Path filePath = localCacheFiles[0];
			//BufferedReader br = new BufferedReader(new FileReader(new File(filePath.toUri().toString())));
			
			//本地调试使用
			BufferedReader br = new BufferedReader(new FileReader("E:/hadoop/output/part-r-00000"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] split = line.toString().split("\t");
				ptnCountMap.put(split[0], Long.parseLong(split[1]));
			}
			br.close();
 
			/**
			 * 仅仅只是为了获取到当前的mapTask要执行编号的编号起点
			 */
			InputSplit inputSplit = context.getInputSplit();
			FileSplit fileSplit = (FileSplit) inputSplit;
			//  name  === "part-r-00000"
			String name = fileSplit.getPath().getName();
			String reduceNo = name.toString().split("-")[2];
			int reduceNumer = Integer.parseInt(reduceNo);
 
			// 假如当前这个reduceNumer编号是 2 。 那就意味着  indexStart的值应该是   reduceNumer 为  0 和 为 1 的 和
			for (int i = 0; i < reduceNumer; i++) {
 
				// i  ===  00000  00022
				String strReduceName = "part-r-" + getReduceTaskResultName(i);
 
				indexStart += ptnCountMap.get(strReduceName);
			}
 
		}
 
		private String getReduceTaskResultName(int i) {
			if (i < 10) {
				return "0000" + i;
			} else if (i < 100) {
				return "000" + i;
			} else if (i < 1000) {
				return "00" + i;
			} else if (i < 10000) {
				return "0" + i;
			} else {
				return "" + i;
			}
		}
 
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			keyOut.set(indexStart);
			context.write(keyOut, value);
			indexStart++;
		}
 
		@Override
		protected void cleanup(Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
 
			//			IOUtils.closeStream(br);
		}
	}
 
 
}