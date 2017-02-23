package cn.edu.gzu.fjbai;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//import cn.edu.gzu.fjbai.Test.Counter;

public class TrafficDataAnalysis {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, LongWritable> {

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			try {
				String[] lineSplit = line.split(",");
				//String date = lineSplit[0];
				 String address = lineSplit[1];
				 String flow = lineSplit[2];
				context.write(new Text(address), new LongWritable(Long.parseLong(flow)));
			} catch (ArrayIndexOutOfBoundsException e) {
				// context.getCounter(Counter.LINESKIP).increment(1);
				return;
			}
		}
	}

	public static class IntSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
		// private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long totalPrice = 0L;
			for (LongWritable value : values) {
				totalPrice += value.get();
			}
			context.write(key, new LongWritable(totalPrice));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "TrafficDataAnalysis");
		job.setJarByClass(TrafficDataAnalysis.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
